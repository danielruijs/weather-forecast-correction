"""Coverage and gaps of the raw data, by source and date.

Writes to ``DATA_DIR/raw/coverage/``:

- ``smhi_daily.parquet``  param, station_id, date, hours (hours with a value, 0-24). A missing hour
  has no row in ``obs/``, so it is never confused with an observed 0.0 mm of rain.
- ``meps_runs.parquet``   run_time, on_disk, grid: one row per expected run since the archive start
- ``summary.md``          per-year coverage, the list of missing MEPS runs, and sizes on disk
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from forecast_correction.config import DATA_DIR
from forecast_correction.data import meps, smhi

COVERAGE_DIR = DATA_DIR / "raw" / "coverage"


def smhi_daily() -> pl.DataFrame:
    frames = []
    for param in smhi.PARAMS:
        if not any(smhi.obs_dir(param).glob("*.parquet")):
            continue
        frames.append(
            smhi.load_observations(param)
            .filter(pl.col("value").is_not_null())
            .group_by("station_id", pl.col("time").dt.date().alias("date"))
            .agg(pl.len().cast(pl.Int8).alias("hours"))
            .with_columns(pl.lit(param).alias("param"))
            .collect()
        )
    return pl.concat(frames).select("param", "station_id", "date", "hours").sort("param", "station_id", "date")


def meps_runs(end: datetime) -> pl.DataFrame:
    expected = pl.datetime_range(meps.FIRST_RUN, end, meps.RUN_INTERVAL, eager=True, time_zone="UTC")
    on_disk = (
        pl.scan_parquet(meps.RUNS_DIR / "**" / "*.parquet")
        .select("run_time", "grid")
        .unique()
        .collect()
    )
    return (
        expected.alias("run_time").to_frame()
        .join(on_disk, on="run_time", how="left")
        .with_columns(pl.col("grid").is_not_null().alias("on_disk"))
        .select("run_time", "on_disk", "grid")
    )


def _size(path: Path) -> str:
    total = sum(f.stat().st_size for f in path.rglob("*.parquet"))
    return f"{total / 1e9:.2f} GB" if total > 1e9 else f"{total / 1e6:.0f} MB"


def _stretches(times: list[datetime]) -> list[str]:
    """Group consecutive missing runs: '2019-03-01 00Z – 03-03 18Z (12 runs)'."""
    groups: list[list[datetime]] = []
    for t in times:
        if groups and t - groups[-1][-1] == meps.RUN_INTERVAL:
            groups[-1].append(t)
        else:
            groups.append([t])
    return [
        f"{g[0]:%Y-%m-%d %HZ}" + (f" – {g[-1]:%m-%d %HZ} ({len(g)} runs)" if len(g) > 1 else "")
        for g in groups
    ]


def write_report() -> Path:
    COVERAGE_DIR.mkdir(parents=True, exist_ok=True)
    daily = smhi_daily()
    daily.write_parquet(COVERAGE_DIR / "smhi_daily.parquet")
    # last expected run: the newest one that is already published
    end = datetime.now(UTC) - meps.AVAILABILITY_DELAY
    runs = meps_runs(end)
    runs.write_parquet(COVERAGE_DIR / "meps_runs.parquet")

    stations = smhi.load_stations()
    lines = [f"# Raw data coverage\n\nGenerated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC.\n"]

    lines.append("\n## SMHI: % of hours with a value, per year\n\n")
    lines.append("Denominator: stations that report the parameter × hours in the year (up to the last observation).\n\n")
    last = daily["date"].max()
    per_year = (
        daily.group_by("param", pl.col("date").dt.year().alias("year"))
        .agg(pl.col("hours").sum().alias("hours"))
        .with_columns(
            n_stations=pl.col("param").map_elements(lambda p: stations[f"has_{p}"].sum(), return_dtype=pl.Int64),
            days=pl.col("year").map_elements(
                lambda y: ((min(datetime(y, 12, 31).date(), last) - datetime(y, 1, 1).date()).days + 1),
                return_dtype=pl.Int64,
            ),
        )
        .with_columns((100 * pl.col("hours") / (pl.col("n_stations") * pl.col("days") * 24)).round(1).alias("pct"))
        .pivot(on="param", index="year", values="pct")
        .sort("year")
    )
    per_year = per_year.select("year", *[p for p in smhi.PARAMS if p in per_year.columns])
    cols = per_year.columns
    lines.append("| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n")
    for row in per_year.iter_rows():
        lines.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |\n")

    missing = runs.filter(~pl.col("on_disk"))["run_time"].to_list()
    lines.append(
        f"\n## MEPS: {runs['on_disk'].sum():,} of {runs.height:,} runs on disk "
        f"({meps.FIRST_RUN:%Y-%m-%d} → {runs['run_time'].max():%Y-%m-%d %HZ}), {len(missing)} missing\n\n"
    )
    lines += [f"- {s}\n" for s in _stretches(missing)]

    lines.append("\n## Size on disk\n\n")
    lines.append(f"- SMHI observations: {_size(smhi.SMHI_DIR / 'obs')}\n")
    lines.append(f"- MEPS runs: {_size(meps.RUNS_DIR)}\n")

    path = COVERAGE_DIR / "summary.md"
    path.write_text("".join(lines), encoding="utf-8")
    return path
