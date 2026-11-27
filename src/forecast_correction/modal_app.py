"""Modal app: hourly update of the raw data on the ``forecast-correction-raw`` Volume.

History is not downloaded on Modal: it is fetched locally with ``fc-data`` and uploaded once
(see ``data/README.md``). The update only adds what's new.

    uv run modal deploy -m forecast_correction.modal_app        # start the hourly schedule

To run an update now, trigger the deployed function (``modal run ...::update`` would start a
separate app that can overlap the scheduled run):

    uv run python -c "import modal; modal.Function.from_name('forecast-correction', 'update').remote()"
"""

import logging

import modal

DATA = "/data"  # Volume mount; FC_DATA_DIR points here, so files land in /data/raw/...

app = modal.App("forecast-correction")
raw = modal.Volume.from_name("forecast-correction-raw", create_if_missing=True)
image = (
    modal.Image.debian_slim(python_version="3.14")
    .uv_sync()
    .env({"FC_DATA_DIR": DATA})
    .add_local_python_source("forecast_correction")
)


# On the hour: a live MEPS run is complete ~2 h 25 min after its start, so the first update after
# it lands ~3 h after the start, matching AVAILABILITY_DELAY. max_containers=1: never two updates
# writing the same monthly files at once.
@app.function(image=image, volumes={DATA: raw}, schedule=modal.Cron("0 * * * *"), timeout=50 * 60, max_containers=1)
def update() -> dict[str, int]:
    from forecast_correction.data import meps, smhi

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    smhi.update_observations()
    raw.commit()  # observations are saved even if the MEPS step fails
    counts = meps.update_forecasts()
    raw.commit()
    return counts
