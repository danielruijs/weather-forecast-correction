"""Data paths."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Override with FC_DATA_DIR, e.g. to point at a Modal volume later.
DATA_DIR = Path(os.environ.get("FC_DATA_DIR", PROJECT_ROOT / "data"))
SMHI_DIR = DATA_DIR / "raw" / "smhi"
MEPS_DIR = DATA_DIR / "raw" / "meps"
