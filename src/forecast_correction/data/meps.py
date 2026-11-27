"""MEPS forecasts from MET Norway's THREDDS server, extracted at SMHI stations.

Per forecast run we download one box covering all stations for each variable (the
server reads whole fields from disk anyway, so a box costs about the same as a point)
and keep the nearest grid cell per station.

Sources, tried in this order:

==================  ======================  ======================================
file                available               notes
==================  ======================  ======================================
live ``det_2_5km``  last ~2 days            complete file appears ~2.4 h after run start
``det_2_5km``       2020-02 → 2023-10,      same values as det_sfc, faster to read;
                    partly 2024 →           files created ~2.4 h after run start
``det_sfc``         2023-10 →               appears ~1.9 h after run start (late steps
                                            may still be filling; an earlier estimate
                                            was ~3.6 h)
``subset``          2016-11 → 2020-02       10-member ensemble, member 0 = control;
                                            file timestamps are from later reprocessing
==================  ======================  ======================================

Output: ``MEPS_DIR/runs/YYYY/MM/meps_YYYYMMDDTHHZ.parquet``, one row per station × lead
(0..MAX_LEAD h), plus ``MEPS_DIR/grids/*.parquet`` with the station → grid-cell mapping.

Publication times can't be recovered from the archive, so every run gets the same documented
estimate, ``available_time = run_time + AVAILABILITY_DELAY`` (see the constant).
"""

import logging
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import netCDF4
import numpy as np
import polars as pl
import requests
from tqdm import tqdm

from forecast_correction.config import MEPS_DIR
from forecast_correction.data.smhi import load_stations

log = logging.getLogger(__name__)

THREDDS = "https://thredds.met.no/thredds/dodsC"
SOURCES = (
    "mepslatest/meps_det_2_5km_{run:%Y%m%dT%H}Z.ncml",
    "meps25epsarchive/{run:%Y/%m/%d}/meps_det_2_5km_{run:%Y%m%dT%H}Z.nc",
    "meps25epsarchive/{run:%Y/%m/%d}/meps_det_sfc_{run:%Y%m%dT%H}Z.ncml",
    "meps25epsarchive/{run:%Y/%m/%d}/meps_subset_2_5km_{run:%Y%m%dT%H}Z.nc",
)
FIRST_RUN = datetime(2016, 11, 8, tzinfo=UTC)
RUN_INTERVAL = timedelta(hours=6)  # 00, 06, 12, 18 UTC
MAX_LEAD = 48  # hours; +24 h from the newest run needs ≤ 33.5 h, from two runs back (missing runs, run-to-run features) ≤ 45.5 h
# When a run counts as available. The live feed (what the deployed system reads) had complete
# runs ~2 h 25 min after run start (catalog times 2026-11-26/27, 06Z on 2026-11-27 at 2 h 26 min);
# the hourly update then picks a run up at the next full hour, ~3 h after its start.
AVAILABILITY_DELAY = timedelta(hours=3)
WORKERS = 4  # more parallel connections get rejected by the server

# name in output -> netCDF variable
VARIABLES = {
    "t2m": "air_temperature_2m",
    "precip_acc": "precipitation_amount_acc",
    "rh": "relative_humidity_2m",
    "mslp": "air_pressure_at_sea_level",
    "x_wind": "x_wind_10m",
    "y_wind": "y_wind_10m",
    "cloud": "cloud_area_fraction",
    "sw_acc": "integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time",
}

RUNS_DIR = MEPS_DIR / "runs"
GRIDS_DIR = MEPS_DIR / "grids"


@dataclass
class Grid:
    """Station → grid-cell mapping for one grid layout."""

    box: tuple[int, int, int, int]  # y0, y1, x0, x1 (end-exclusive)
    iy: np.ndarray  # station cell row, relative to the box
    ix: np.ndarray  # station cell column, relative to the box
    cos_a: np.ndarray  # rotation of the grid x-axis relative to east
    sin_a: np.ndarray


def _grid_key(ds: netCDF4.Dataset) -> str:
    return f"{len(ds['y'])}x{len(ds['x'])}_{float(ds['x'][0]):.0f}_{float(ds['y'][0]):.0f}"


def _build_grid(ds: netCDF4.Dataset, stations: pl.DataFrame) -> Grid:
    lat = np.asarray(ds["latitude"][:], dtype=np.float64)
    lon = np.asarray(ds["longitude"][:], dtype=np.float64)

    iy, ix, dist = [], [], []
    for la, lo in zip(stations["lat"], stations["lon"], strict=True):
        d2 = (lat - la) ** 2 + ((lon - lo) * math.cos(math.radians(la))) ** 2
        j, i = np.unravel_index(np.argmin(d2), d2.shape)
        iy.append(j)
        ix.append(i)
        dist.append(math.sqrt(d2[j, i]) * 111.2)
    iy, ix = np.array(iy), np.array(ix)

    # angle of the grid x-axis relative to east, from neighbouring cells
    dlon = (lon[iy, ix + 1] - lon[iy, ix - 1]) * np.cos(np.radians(lat[iy, ix]))
    dlat = lat[iy, ix + 1] - lat[iy, ix - 1]
    angle = np.arctan2(dlat, dlon)

    # box with a 1-cell margin for the 3x3 neighbourhood
    y0, x0 = iy.min() - 1, ix.min() - 1
    grid = Grid((y0, iy.max() + 2, x0, ix.max() + 2), iy - y0, ix - x0, np.cos(angle), np.sin(angle))

    geopotential = _read_box(ds, "surface_geopotential", grid, n_time=1)[0]
    GRIDS_DIR.mkdir(parents=True, exist_ok=True)
    path = GRIDS_DIR / f"{_grid_key(ds)}.parquet"
    tmp = path.with_suffix(f".{os.getpid()}.tmp")  # every worker builds the grid; write atomically
    pl.DataFrame(
        {
            "station_id": stations["station_id"],
            "cell_lat": lat[iy, ix],
            "cell_lon": lon[iy, ix],
            "dist_km": dist,
            "cell_elevation": geopotential[grid.iy, grid.ix] / 9.80665,  # model terrain height (m)
        }
    ).write_parquet(tmp)
    tmp.replace(path)  # not rename: on Windows it fails when the target exists
    return grid


def _read_box(ds: netCDF4.Dataset, name: str, grid: Grid, n_time: int) -> np.ndarray:
    """Read (time, y, x) for the station box; takes index 0 of singleton levels and ensemble member."""
    y0, y1, x0, x1 = grid.box
    dims = {"time": slice(0, n_time), "y": slice(y0, y1), "x": slice(x0, x1)}
    var = ds[name]
    arr = var[tuple(dims.get(d, 0) for d in var.dimensions)]
    return np.ma.filled(arr.astype(np.float32), np.nan)


def run_path(run: datetime) -> Path:
    return RUNS_DIR / f"{run:%Y/%m}" / f"meps_{run:%Y%m%dT%H}Z.parquet"


_grids: dict[str, Grid] = {}  # per worker process


def _read_run(run: datetime, stations: pl.DataFrame) -> tuple[Grid, dict[str, np.ndarray]] | None:
    """Read all variables from the first source with a complete run; None if no source has one.

    A live run that is still being published has fewer time steps or empty late steps;
    it is skipped, so it is downloaded again on the next invocation.
    """
    n_time = MAX_LEAD + 1
    for source in SOURCES:
        url = f"{THREDDS}/{source.format(run=run)}"
        # existence check over HTTP (netCDF-C would print the 404 page to stderr)
        r = requests.head(f"{url}.dds", timeout=60)
        if r.status_code == 404:
            continue
        r.raise_for_status()
        with netCDF4.Dataset(url) as ds:
            if len(ds["time"]) < n_time:
                continue
            key = _grid_key(ds)
            if key not in _grids:
                _grids[key] = _build_grid(ds, stations)
            grid = _grids[key]
            raw = {out: _read_box(ds, name, grid, n_time) for out, name in VARIABLES.items()}
        if not any(np.isnan(a).all(axis=(1, 2)).any() for a in raw.values()):
            return grid, raw
    return None


def extract_run(run: datetime, stations: pl.DataFrame) -> pl.DataFrame | None:
    """Download one run; one row per station × lead, or None if no complete run is available."""
    result = _read_run(run, stations)
    if result is None:
        return None
    grid, raw = result
    n_time = MAX_LEAD + 1

    iy, ix = grid.iy, grid.ix

    def at_station(a: np.ndarray) -> np.ndarray:  # (time, y, x) -> (time, station)
        return a[:, iy, ix]

    def hourly(acc: np.ndarray) -> np.ndarray:  # de-accumulate; lead 0 has no previous hour
        out = np.full_like(acc, np.nan)
        out[1:] = np.diff(acc, axis=0)
        return out

    precip = np.clip(hourly(raw["precip_acc"]), 0, None)
    x_wind, y_wind = at_station(raw["x_wind"]), at_station(raw["y_wind"])
    columns = {
        "t2m": at_station(raw["t2m"]) - 273.15,  # °C
        "precip": at_station(precip),  # mm in the hour ending at valid_time
        # 3x3 neighbourhood mean (forecast showers are often slightly misplaced)
        "precip_3x3": np.mean([precip[:, iy + dy, ix + dx] for dy in (-1, 0, 1) for dx in (-1, 0, 1)], axis=0),
        "rh": at_station(raw["rh"]) * 100,  # %
        "mslp": at_station(raw["mslp"]) / 100,  # hPa
        "wind_u": x_wind * grid.cos_a - y_wind * grid.sin_a,  # m/s, eastward
        "wind_v": x_wind * grid.sin_a + y_wind * grid.cos_a,  # m/s, northward
        "cloud": at_station(raw["cloud"]),  # 0..1
        "sw": np.clip(at_station(hourly(raw["sw_acc"])) / 3600, 0, None),  # W/m², mean over the hour
    }

    n_stations = stations.height
    return pl.DataFrame(
        {
            "run_time": [run] * (n_time * n_stations),
            "lead": np.repeat(np.arange(n_time, dtype=np.int8), n_stations),
            "station_id": np.tile(stations["station_id"].to_numpy(), n_time),
            **{k: a.reshape(-1).astype(np.float32) for k, a in columns.items()},
        }
    ).select(
        "run_time",
        (pl.col("run_time") + AVAILABILITY_DELAY).alias("available_time"),
        (pl.col("run_time") + pl.duration(hours=pl.col("lead"))).alias("valid_time"),
        "lead",
        "station_id",
        pl.col(*columns).fill_nan(None),  # precip/sw at lead 0
    )


def _fetch_run(run: datetime, stations: pl.DataFrame, retries: int = 4) -> str:
    """Download and write one run; returns "ok", "missing" or "failed"."""
    for attempt in range(retries):
        try:
            df = extract_run(run, stations)
            break
        except Exception as e:  # network hiccups, server throttling
            if attempt == retries - 1:
                log.error("run %s failed: %r", run, e)
                return "failed"
            time.sleep(10 * 2**attempt)
    if df is None:
        return "missing"
    path = run_path(run)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.write_parquet(tmp)
    # atomic, so an interrupted write is never mistaken for a finished run;
    # replace, not rename: on Windows rename fails when the target exists
    tmp.replace(path)
    return "ok"


def fetch_forecasts(start: datetime = FIRST_RUN, end: datetime | None = None) -> None:
    """Download all runs in [start, end] not yet on disk, newest first."""
    end = end or datetime.now(UTC)
    first = start + (FIRST_RUN - start) % RUN_INTERVAL  # round up to a run time
    runs = [first + i * RUN_INTERVAL for i in range(int((end - first) / RUN_INTERVAL) + 1)]
    todo = [r for r in reversed(runs) if not run_path(r).exists()]
    log.info("%d runs to download", len(todo))

    stations = load_stations()
    counts = {"ok": 0, "missing": 0, "failed": 0}
    with ProcessPoolExecutor(WORKERS) as ex:
        futures = [ex.submit(_fetch_run, run, stations) for run in todo]
        bar = tqdm(as_completed(futures), total=len(futures), desc="MEPS runs")
        for future in bar:
            counts[future.result()] += 1
            bar.set_postfix(counts)
    log.info("done: %s", counts)
