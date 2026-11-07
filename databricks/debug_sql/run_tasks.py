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
# Each Databricks task belongs to a job. Start the chosen graph here.
from databricks.sdk import WorkspaceClient
from job_sync import render_settings, resolve_job
dbutils.widgets.dropdown('job','delta',['delta','full','ingestion_delta','ingestion_full'])
dbutils.widgets.text('start_date','')
dbutils.widgets.text('end_date','')
key = dbutils.widgets.get('job')
client = WorkspaceClient()
job_id = resolve_job(client,config,key,render_settings(config,key))
if job_id is None:
    raise ValueError('Deploy the job first.')
params = {k:dbutils.widgets.get(k) for k in ['start_date','end_date']} if key.startswith('ingestion_') else None
run = client.jobs.run_now(job_id=job_id,job_parameters=params)
print(f'Started job_id={job_id}, run_id={run.run_id}; wait for completion in Jobs before the next operation.')
