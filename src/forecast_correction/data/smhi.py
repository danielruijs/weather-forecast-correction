"""SMHI station metadata and hourly observations.

API docs: https://opendata.smhi.se/apidocs/metobs/

Layout on disk (under ``SMHI_DIR``):

- ``stations.parquet``              one row per selected station (latest position)
- ``station_positions.parquet``     position history per station (stations can move)
- ``obs/{param}/{station}.parquet`` columns ``time`` (UTC), ``value``, ``quality``, ``source``
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

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


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


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


def _download_csv(url: str, source: str) -> pl.DataFrame:
    """Parse an SMHI metobs CSV: a variable-length header, then `Datum;Tid (UTC);value;Kvalitet` rows."""
    r = _get(url)
    lines = r.text.splitlines() if r is not None else []
    start = next((i + 1 for i, line in enumerate(lines) if line.startswith("Datum;Tid (UTC)")), len(lines))
    rows = [line.split(";")[:4] for line in lines[start:]]
    rows = [row for row in rows if len(row) == 4 and row[0] >= OBS_START]
    return pl.DataFrame(rows, schema=["date", "tod", "value", "quality"], orient="row").select(
        (pl.col("date") + " " + pl.col("tod")).str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC").alias("time"),
        pl.col("value").cast(pl.Float32, strict=False),  # occasional empty values -> null
        pl.col("quality").cast(pl.Categorical),
        pl.lit(source).cast(pl.Categorical).alias("source"),
    )


def fetch_station_param(station_id: int, param: str) -> None:
    """Download one station × parameter: quality-controlled archive + unchecked latest months."""
    base = f"{API}/parameter/{PARAMS[param]}/station/{station_id}/period"
    archive = _download_csv(f"{base}/corrected-archive/data.csv", "archive")
    latest = _download_csv(f"{base}/latest-months/data.csv", "latest")
    if archive.height:  # archive is quality-controlled -> wins where both cover the same period
        latest = latest.filter(pl.col("time") > archive["time"].max())

    out = SMHI_DIR / "obs" / param / f"{station_id}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.concat([archive, latest]).sort("time").write_parquet(out)


def fetch_observations() -> None:
    jobs = [
        (row["station_id"], p)
        for row in load_stations().iter_rows(named=True)
        for p in PARAMS
        if row[f"has_{p}"]
    ]
    with ThreadPoolExecutor(8) as ex:
        futures = {ex.submit(fetch_station_param, s, p): (s, p) for s, p in jobs}
        for future in tqdm(as_completed(futures), total=len(futures), desc="observations"):
            if e := future.exception():
                log.error("failed %s: %r (re-run to retry)", futures[future], e)
