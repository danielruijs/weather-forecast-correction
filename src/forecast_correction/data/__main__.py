"""Download raw data.

    uv run fc-data stations                 # station list (run first)
    uv run fc-data obs                      # SMHI observations
    uv run fc-data meps [--start] [--end]   # MEPS forecast runs (resumable)
    uv run fc-data coverage                 # coverage and gaps report
"""

import argparse
import logging
from datetime import UTC, datetime

from forecast_correction.data import coverage, meps, smhi


def _utc(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser(prog="fc-data", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stations", help="SMHI station list and metadata")
    sub.add_parser("obs", help="SMHI hourly observations")
    p = sub.add_parser("meps", help="MEPS forecast runs, newest first")
    p.add_argument("--start", type=_utc, default=meps.FIRST_RUN, help="first run, UTC (default: start of archive)")
    p.add_argument("--end", type=_utc, help="last run, UTC (default: now)")
    sub.add_parser("coverage", help="write the coverage and gaps report")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "stations":
        smhi.fetch_stations()
    elif args.cmd == "obs":
        smhi.fetch_observations()
    elif args.cmd == "meps":
        meps.fetch_forecasts(args.start, args.end)
    else:
        print(coverage.write_report())


if __name__ == "__main__":
    main()
