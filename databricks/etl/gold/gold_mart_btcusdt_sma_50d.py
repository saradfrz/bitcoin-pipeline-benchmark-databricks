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
from pipeline import Pipeline
pipeline = Pipeline(spark, dbutils, config)
dbutils.widgets.dropdown('load_mode','delta',['delta','full'])
pipeline.gold('sma_50d',dbutils.widgets.get('load_mode'))
