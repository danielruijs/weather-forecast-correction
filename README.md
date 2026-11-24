# weather-forecast-correction

## Data

Raw data goes to `data/` (gitignored; override with `FC_DATA_DIR`).

```bash
uv run fc-data stations                    # 142 SMHI stations with hourly T + P (run first)
uv run fc-data obs                         # SMHI observations 2016 → now, a few minutes
uv run fc-data meps                        # MEPS runs 2016-11 → now, newest first, ~2.5 days
uv run fc-data meps --start 2026-11-01     # only part of the archive
```

`fc-data meps` can be stopped and restarted at any time: runs already on disk are skipped. Recent runs come
from the live feed (`mepslatest`), older ones from the archive.
