import json
from pathlib import Path

_config_path = Path(__file__).resolve().parent / "config.json"

with _config_path.open(encoding="utf-8") as file:
    config = json.load(file)