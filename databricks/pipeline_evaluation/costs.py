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
from evaluation import cost_report
settings = config['evaluation']
dbutils.widgets.text('cost_start',settings['cost_start'])
dbutils.widgets.text('cost_end',settings['cost_end'])
cost_report(pipeline,display,dbutils.widgets.get('cost_start'),dbutils.widgets.get('cost_end'),settings['currency'])
