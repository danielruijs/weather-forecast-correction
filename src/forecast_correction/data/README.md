# Raw data

Download code for the two sources. Files go to `data/raw/` (or `$FC_DATA_DIR/raw/`); all times are UTC.

| file | what |
|---|---|
| `smhi.py` | SMHI stations and hourly observations |
| `meps.py` | MEPS forecasts (MET Norway THREDDS), cut out at the stations |
| `coverage.py` | coverage and gaps report |
| `__main__.py` | the `fc-data` command |
| `../modal_app.py` | hourly update on Modal |

## Getting the data

History is downloaded **once, locally**, then uploaded to the Modal Volume `forecast-correction-raw`.
After that, Modal's hourly `update` adds new observations and forecast runs to the Volume. It
normally fetches the last ~24 h of observations, but switches to the last ~4 months when the newest
observation on the Volume is older than 20 h, and it fetches any MEPS run of the last 7 days that is
missing. So gaps from an outage, or from uploading days after downloading, fill themselves (up to ~4
months for observations, 7 days for forecasts).

```
uv run fc-data stations      # 10 s
uv run fc-data obs           # 6 min
uv run fc-data meps          # 52 h (the server allows 4 parallel downloads); resumable, re-run to retry failures
uv run fc-data coverage      # data/raw/coverage/: gaps per source and date

uv run modal volume create forecast-correction-raw              # once
uv run modal volume put forecast-correction-raw data/raw /raw   # upload, 3-10 min; appears when done
uv run modal deploy -m forecast_correction.modal_app            # start the hourly update
```

To work locally with the up-to-date data from Modal (lands in `data/raw/`, `--force` overwrites):

```
uv run modal volume get --force forecast-correction-raw /raw data
```

In Git Bash, run `export MSYS_NO_PATHCONV=1` first, or it rewrites `/raw` to `C:/Program Files/Git/raw`.

## SMHI — `data/raw/smhi/` (88 MB)

**`stations.parquet`**: the 142 stations that report both hourly temperature and precipitation.
`has_*` says which other variables a station has.

```
station_id  name                            lat    lon    elevation  has_t2m  has_precip  has_rh  has_mslp  has_wind_speed  has_wind_dir
98230       Stockholm-Observatoriekullen A  59.34  18.05  43.13      true     true        true    true      false           false
188790      Abisko Aut                      68.35  18.82  392.24     true     true        true    true      true            true
```

**`station_positions.parquet`**: where each station was in each period (stations can move).

```
station_id  from                 to                   lat    lon    elevation
98230       1996-10-01 00:00:00  2026-11-24 21:20:16  59.34  18.05  43.13
```

**`obs/{param}/YYYY-MM.parquet`**: one file per variable and month with all stations, one row per
station and hour, from 2016-01-01.

- `param`:
  - `t2m`: air temperature 2 m above ground, °C, measured at `time`
  - `precip`: total precipitation in the hour ending at `time`, in mm (1 mm = 1 litre per m²);
    0.0 means it was dry
  - `rh`: relative humidity 2 m above ground, % (100 = air saturated with water vapour, e.g. fog
    or rain), measured at `time`
  - `mslp`: air pressure converted to sea level (so stations at different heights compare), hPa,
    measured at `time`; normal is ~1012 in Sweden, low = unsettled/stormy, high = calm
  - `wind_speed`: 10-minute mean wind speed 10 m above ground, m/s
  - `wind_dir`: 10-minute mean direction the wind comes *from*, degrees: 90 = east, 180 = south,
    360 = north, **0 = calm** (speed 0), not north
- `quality`: `G` (checked) or `Y` (suspect)
- `source`: `archive` (quality-controlled, ends ~3 months ago) or `latest` (last ~4 months, unchecked)

```
station_id  time                 value  quality  source
98230       2025-08-15 19:00:00  21.9   G        archive     (obs/t2m/2025-08)
98230       2025-08-15 20:00:00  14.7   G        archive
98230       2024-04-22 11:00:00  57.0   Y        archive     (obs/rh/2024-04)
98230       2026-11-24 21:00:00  0.0    G        latest      (obs/precip/2026-11)
```

If an hour appears twice, `archive` wins over `latest`, then `G` over `Y` (the archive has both for
pressure at 8 stations, 2025-02-05 → 02-22). Missing hours are missing rows, so a gap is never an
observed 0.0 mm. Not every station has every parameter (e.g. `mslp` exists for 96 of 142).

## MEPS — `data/raw/meps/` (2.4 GB)

**`runs/YYYY/MM/meps_YYYYMMDDTHHZ.parquet`**: one file per forecast run (00, 06, 12, 18 UTC,
2016-11-08 →; 14,614 runs), one row per station × lead hour 0–48 (6,958 rows).

```
run_time          available_time    grid         valid_time        lead  station_id  t2m    precip  precip_3x3  rh     mslp     wind_u  wind_v  cloud  sw
2025-08-14 18:00  2025-08-14 21:00  1069x949_…   2025-08-14 18:00  0     98230       24.11  null    null        49.66  1022.09  1.43    4.61    1.00   null
2025-08-14 18:00  2025-08-14 21:00  1069x949_…   2025-08-15 19:00  25    98230       21.18  3.28    2.93        85.53  1009.01  2.78    2.07    1.00   5.27
2025-08-14 18:00  2025-08-14 21:00  1069x949_…   2025-08-15 20:00  26    98230       21.86  0.00    0.01        73.21  1009.12  4.80    -0.29   0.98   0.00
```

- `available_time` = `run_time` + 3 h: when the run counts as published (see `AVAILABILITY_DELAY`).
- `grid`: which file in `grids/` holds this run's station cells (full value `1069x949_-1060084_-1332518`).
- `valid_time` = `run_time` + `lead` hours: the hour the forecast is for.
- `t2m` °C, `rh` %, `mslp` hPa, `wind_u`/`wind_v` m/s (east/north), `cloud` 0–1.
- `precip` mm in the hour ending at `valid_time`; `precip_3x3` the same, averaged over the 3×3 cells
  around the station; `sw` mean shortwave radiation over that hour, W/m². These three are `null` at lead 0.

**`grids/*.parquet`**: the grid cell used for each station, one file per grid layout. MET Norway
changed the MEPS grid twice (all 2.5 km); the file name is the grid size (y×x cells) plus its origin:

| grid | runs | station cells |
|---|---|---|
| `949x739_…` | 2016-11-08 → 2017-09-06 00Z | |
| `949x889_…` | 2017-09-06 06Z → 2020-02-04 06Z | domain widened; 127 of 142 stations keep their cell |
| `1069x949_…` | 2020-02-04 12Z → | new grid; every station gets a new cell (terrain height up to 69 m different) |

`cell_elevation` is the model's terrain height, which can differ from the station's.

```
station_id  cell_lat  cell_lon  dist_km  cell_elevation
98230       59.33     18.03     1.62     21.94
```

**Gaps:** 71 of 14,685 runs (to 2026-11-27 00Z) are missing: 57 not in the archive, 14 damaged 2017
files. From 2023-11 on, only 3 are missing (2025-05-13 12Z, 2026-09-02 00Z and 06Z). The full list is
in `coverage/summary.md`.

## Coverage — `data/raw/coverage/`

Written by `fc-data coverage`:

- `smhi_daily.parquet`: param, station_id, date, hours with a value (0–24)
- `meps_runs.parquet`: every expected run with `on_disk` and `grid`
- `summary.md`: % of hours per variable and year, the missing MEPS runs, sizes
