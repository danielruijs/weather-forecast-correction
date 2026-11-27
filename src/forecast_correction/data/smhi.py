"""SMHI station metadata and hourly observations.

API docs: https://opendata.smhi.se/apidocs/metobs/

Layout on disk (under ``SMHI_DIR``):

- ``stations.parquet``              one row per selected station (latest position)
- ``station_positions.parquet``     position history per station (stations can move)
- ``obs/{param}/YYYY-MM.parquet``    all stations for one month: ``station_id``, ``time`` (UTC),
  ``value``, ``quality``, ``source``

``fetch_observations`` downloads the full history (run locally once, then uploaded to Modal); the
hourly update on Modal (``update_observations``) downloads the latest data. Both merge into the
monthly files.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import requests
from tqdm import tqdm

from forecast_correction.config import SMHI_DIR

log = logging.getLogger(__name__)

API = "https://opendata-download-metobs.smhi.se/api/version/1.0"
OBS_START = "2016-01-01"  # MEPS archive starts 2016-11; earlier observations aren't needed

# name used on disk -> SMHI parameter id (all hourly)
PARAMS = {
    "t2m": 1,  # air temperature, instantaneous (°C)
    "precip": 7,  # precipitation, 1 h sum (mm)
    "rh": 6,  # relative humidity, instantaneous (%)
    "mslp": 9,  # sea-level pressure, instantaneous (hPa)
    "wind_speed": 4,  # 10 min mean (m/s)
    "wind_dir": 3,  # 10 min mean (degrees)
}
REQUIRED_PARAMS = ("t2m", "precip")  # a station is selected when it is active for both


def _get(url: str, retries: int = 5) -> requests.Response | None:
    """GET with retry; returns None on 404 (no data for this station/period)."""
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _ms_to_dt(ms: int) -> datetime:
    # not datetime.fromtimestamp: on Windows it rejects negative (pre-1970) timestamps
    return _EPOCH + timedelta(milliseconds=ms)


def fetch_stations() -> None:
    """Select active stations with all REQUIRED_PARAMS and save metadata + positions."""
    active = {
        name: {s["id"]: s for s in _get(f"{API}/parameter/{pid}.json").json()["station"] if s["active"]}
        for name, pid in PARAMS.items()
    }
    ids = sorted(set.intersection(*(set(active[p]) for p in REQUIRED_PARAMS)))
    log.info("%d stations active for %s", len(ids), ", ".join(REQUIRED_PARAMS))

    rows, positions = [], []
    for sid in tqdm(ids, desc="station metadata"):
        meta = _get(f"{API}/parameter/{PARAMS['t2m']}/station/{sid}.json").json()
        latest = max(meta["position"], key=lambda p: p["from"])
        rows.append(
            {
                "station_id": sid,
                "name": active["t2m"][sid]["name"],
                "lat": latest["latitude"],
                "lon": latest["longitude"],
                "elevation": latest["height"],
                **{f"has_{p}": sid in active[p] for p in PARAMS},
            }
        )
        positions += [
            {
                "station_id": sid,
                "from": _ms_to_dt(p["from"]),
                "to": _ms_to_dt(p["to"]),
                "lat": p["latitude"],
                "lon": p["longitude"],
                "elevation": p["height"],
            }
            for p in meta["position"]
        ]

    SMHI_DIR.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(SMHI_DIR / "stations.parquet")
    pl.DataFrame(positions).write_parquet(SMHI_DIR / "station_positions.parquet")


def load_stations() -> pl.DataFrame:
    return pl.read_parquet(SMHI_DIR / "stations.parquet")


OBS_SCHEMA = {
    "station_id": pl.Int64,
    "time": pl.Datetime("us", "UTC"),
    "value": pl.Float32,
    "quality": pl.String,
    "source": pl.String,
}


def _download_csv(url: str, station_id: int, source: str) -> pl.DataFrame:
    """Parse an SMHI metobs CSV: a variable-length header, then `Datum;Tid (UTC);value;Kvalitet` rows."""
    r = _get(url)
    lines = r.text.splitlines() if r is not None else []
    start = next((i + 1 for i, line in enumerate(lines) if line.startswith("Datum;Tid (UTC)")), len(lines))
    rows = [line.split(";")[:4] for line in lines[start:]]
    rows = [row for row in rows if len(row) == 4 and row[0] >= OBS_START]
    n = len(rows)
    return pl.DataFrame(
        {
            "station_id": [station_id] * n,
            "time": pl.Series([f"{r[0]} {r[1]}" for r in rows], dtype=pl.String).str.to_datetime(
                "%Y-%m-%d %H:%M:%S", time_zone="UTC"
            ),
            "value": pl.Series([r[2] for r in rows], dtype=pl.String).cast(pl.Float32, strict=False),  # empty -> null
            "quality": [r[3] for r in rows],
            "source": [source] * n,
        },
        schema=OBS_SCHEMA,
    )


def fetch_station_param(station_id: int, param: str) -> pl.DataFrame:
    """Download one station × parameter: quality-controlled archive + unchecked latest months."""
    base = f"{API}/parameter/{PARAMS[param]}/station/{station_id}/period"
    archive = _download_csv(f"{base}/corrected-archive/data.csv", station_id, "archive")
    latest = _download_csv(f"{base}/latest-months/data.csv", station_id, "latest")
    if archive.height:  # archive is quality-controlled -> wins where both cover the same period
        latest = latest.filter(pl.col("time") > archive["time"].max())
    return pl.concat([archive, latest])


def fetch_latest(station_id: int, param: str, period: str) -> pl.DataFrame:
    """``latest-day`` (~24 h) or ``latest-months`` (~4 months) of one station × parameter."""
    url = f"{API}/parameter/{PARAMS[param]}/station/{station_id}/period/{period}/data.csv"
    return _download_csv(url, station_id, "latest")


def dedupe(obs: pl.DataFrame) -> pl.DataFrame:
    """One row per station and hour, sorted by station and time.

    Preference: archive over latest (quality-controlled), then checked "G" over suspect "Y" (the
    archive holds both for pressure at 8 stations, 2025-02-05 → 02-22), then the most recently
    added row.
    """
    keys = ["station_id", "time"]
    return (
        obs.with_row_index("_order")
        .sort(
            pl.col("source") != "archive", pl.col("quality") != "G", "_order", descending=[False, False, True]
        )
        .unique(keys, keep="first")
        .drop("_order")
        .sort(keys)
    )


def obs_dir(param: str) -> Path:
    return SMHI_DIR / "obs" / param


def write_observations(param: str, new: pl.DataFrame) -> None:
    """Merge rows into the monthly files ``obs/{param}/YYYY-MM.parquet``."""
    if new.is_empty():
        return
    obs_dir(param).mkdir(parents=True, exist_ok=True)
    for (month,), part in new.group_by(pl.col("time").dt.strftime("%Y-%m")):
        path = obs_dir(param) / f"{month}.parquet"
        if path.exists():
            part = pl.concat([pl.read_parquet(path), part])
        tmp = path.with_suffix(".tmp")
        dedupe(part).write_parquet(tmp)
        tmp.replace(path)  # atomic, so readers never see a half-written month


def load_observations(param: str) -> pl.LazyFrame:
    return pl.scan_parquet(obs_dir(param) / "*.parquet")


def _jobs() -> list[tuple[int, str]]:
    return [(row["station_id"], p) for p in PARAMS for row in load_stations().iter_rows(named=True) if row[f"has_{p}"]]


def _fetch_all(jobs: list[tuple[int, str]], fetch, desc: str) -> dict[str, list[pl.DataFrame]]:
    """Run ``fetch(station, param)`` for all jobs; returns the frames per parameter."""
    frames: dict[str, list[pl.DataFrame]] = {p: [] for p in PARAMS}
    with ThreadPoolExecutor(8) as ex:
        futures = {ex.submit(fetch, s, p): (s, p) for s, p in jobs}
        for future in tqdm(as_completed(futures), total=len(futures), desc=desc):
            if e := future.exception():
                log.error("failed %s: %r (re-run to retry)", futures[future], e)
            else:
                frames[futures[future][1]].append(future.result())
    return frames


def fetch_observations() -> None:
    """Full history per station × parameter (run locally once, then uploaded)."""
    jobs = _jobs()
    for param in PARAMS:  # one parameter at a time keeps memory to ~one parameter's history
        frames = _fetch_all([j for j in jobs if j[1] == param], fetch_station_param, f"observations {param}")[param]
        if frames:
            write_observations(param, pl.concat(frames))


def _newest_observation() -> datetime | None:
    """Newest hour on disk, taking the parameter that is furthest behind."""
    newest = []
    for param in PARAMS:
        months = sorted(obs_dir(param).glob("*.parquet"))
        if not months:
            return None
        newest.append(pl.read_parquet(months[-1], columns=["time"])["time"].max())
    return min(newest)


def update_observations() -> None:
    """Hourly update: the last ~24 h, or the last ~4 months if the data on disk is older than that
    (after an outage, or a late upload), so gaps fill themselves."""
    newest = _newest_observation()
    period = "latest-day" if newest and datetime.now(UTC) - newest < timedelta(hours=20) else "latest-months"
    log.info("newest observation %s -> fetching %s", newest, period)
    frames = _fetch_all(_jobs(), lambda s, p: fetch_latest(s, p, period), f"observations {period}")
    for param, fs in frames.items():
        if fs:
            write_observations(param, pl.concat(fs))
