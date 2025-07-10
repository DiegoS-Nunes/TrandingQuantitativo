import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "cvm_data.db"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)