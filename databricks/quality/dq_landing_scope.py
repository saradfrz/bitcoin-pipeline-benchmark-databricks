# Databricks notebook source
# Python source notebook. Run from the cloned Databricks Git folder (DBR 14.3+ / serverless).
from pathlib import Path
import json
import sys

# COMMAND ----------
# No __file__ dependency: notebook CWD is its containing Git folder.
_candidates = [Path.cwd(), *Path.cwd().parents]
_repo = next((p for p in _candidates if (p / "config.json").is_file() and (p / "databricks/src/configuration.py").is_file()), None)
if _repo is None:
    raise RuntimeError("Run this notebook from the project Git folder so config.json can be located.")
sys.path.insert(0, str(_repo / "databricks/src"))
from configuration import load_config
config = load_config(_repo)

# COMMAND ----------
# Interactive read-only inspection; job roots publish the same result as a task value.
from configuration import landing_scope
print(landing_scope([(f.name,f.size) for f in dbutils.fs.ls(config['landing']) if not f.isDir()], config['symbol']))
