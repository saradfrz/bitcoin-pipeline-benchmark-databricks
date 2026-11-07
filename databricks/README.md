# Databricks BTCUSDT pipeline

This standalone repository contains the Databricks BTCUSDT pipeline. Executable notebooks are Databricks `.py` source notebooks; reusable code is under `src`. All transformations use PySpark and Delta Lake. SQL strings are used only for namespace/table DDL, table reset and metadata inspection inside Python.

## First deployment

1. Commit this project to GitHub, then clone or pull it into a Databricks Git folder. Replace the old `databricks` directory as a directory, rather than merging it and leaving the old notebooks/YAML behind.
2. Edit root `config.json`. Defaults create `BTC_TRADING`, the four schemas and the eight requested tables. `databricks_project_root` may remain blank to discover the current checkout, or contain its absolute Databricks folder path. It must point to the checkout you are deploying. There are no embedded tokens.
3. Use Unity Catalog-enabled Databricks Runtime 14.3+ or serverless, with permissions to create catalog/schemas/tables/volume and manage jobs. The default job environment is serverless environment version `6`, retained from the original project and editable in configuration. Enable outbound access to `data.binance.vision` for ingestion. Install `requests`, `pyyaml` and `databricks-sdk` in the interactive notebook environment used for deployment. Job environments declare their dependencies in YAML. PySpark and Delta are supplied by Databricks; do not install local test requirements on a Databricks cluster.
4. Run `setup_init/setup_environment.py`. It creates namespaces and empty tables, then creates or synchronizes all four jobs. Alternatively run numbered `setup_init/01` through `05` notebooks in order.
5. Copy the printed job IDs into `databricks.jobs.ids` in root `config.json` and commit them. Exact-name discovery with a project ownership tag also supports subsequent synchronization.
6. Run **ingestion_full**, wait for success, then run **full**. A first full load is required when migrating from the previous Databricks version: this version uses a new Auto Loader checkpoint and expects its own schemas. If an existing table has an incompatible schema, migrate or recreate that table deliberately before setup; setup never silently drops existing tables.
7. For daily operation run **ingestion_delta**, wait for success, then **delta**. All jobs are manual by default. Add a schedule to the versioned YAML if desired.

Do not overlap full/delta processing, either ingestion job, retention, or deployments against the same landing volume/tables. `max_concurrent_runs: 1` prevents overlap within each job, not between different jobs. The four jobs are deliberately independent; ingestion does not launch processing automatically.

## Jobs and date widgets

| Job | Default input | Custom range | Processing |
| --- | --- | --- | --- |
| ingestion_full | 2026-01-01 through yesterday UTC | `start_date` / `end_date`, inclusive YYYY-MM-DD | Download ZIP and extract CSV |
| ingestion_delta | Yesterday UTC only | Same widgets / job parameters | Download ZIP and extract CSV |
| full | All retained landing CSVs | Range is defined by landing contents | Validate, reset, reload, Silver, DQ, four Gold models |
| delta | New files plus summary backfill | Range is defined by landing contents | Incremental processing, DQ, Gold, retention |

Both ingestion jobs retain the original notebook locations, converted to `.py`: `etl/ingestion.py` and `historical_data/Binance_API_Historical_Data_Ingestion.py`. Blank job date parameters resolve when the run starts. Interactive notebooks display their initial default dates in widgets; clear the values to recompute defaults if reusing a notebook across days. Existing nonempty files are reused. A missing archive (including a Binance 404) causes an incomplete-ingestion failure; already downloaded days remain available for retry. The downloader uses the existing Binance archive REST endpoint; it does not request millions of individual trades through the live trading API and does not convert CSV to Parquet.

A custom ingestion range adds files; it does not isolate a processing run to those dates, remove other landing files, or bypass quality checks. Landing coverage must be contiguous. The delta summary check requires all days from the earliest landing date through yesterday UTC, so a deliberately incomplete historical dataset should first be processed with **full**, or completed through yesterday before running **delta**. Archives before 2025 are rejected by ingestion because the current timestamp contract is microseconds.

## GitHub YAML synchronization

Source definitions are `orchestration/full.yml`, `delta.yml`, `ingestion_full.yml`, and `ingestion_delta.yml`. They use the same `resources.jobs` wrapper as the original notebook. `orchestration/manage_binance_job.py` is the converted synchronizer; it renders configuration variables and calls Jobs API create/reset. A reset replaces task definitions, including dependency changes and removed tasks, while retaining job IDs and run history. It validates all YAML files and job targets before mutations and rejects deployment when an existing managed job is active. Changes across four API calls are not atomic; rerunning after a partial deployment converges to the YAML definitions.

After changes, commit/push, pull the desired GitHub revision into the Databricks Git folder, then run `setup_redeploy/01_redeploy_orchestration.py`. No truncation is performed during redeployment. Workspace notebook paths intentionally point at this checkout, matching the original deployment approach. A GitHub push by itself does not update the Databricks checkout or deploy Jobs API settings. Keep the checkout unchanged during a run. This is a small Jobs API synchronizer, not a Databricks bundle deployment engine. Unrelated jobs are never deleted; remove obsolete legacy jobs separately if you no longer use them.

## Data and recovery semantics

Bronze uses Auto Loader `availableNow` to read headerless CSVs from the managed Volume. Full validates landing first, truncates all seven data tables, then removes the dedicated checkpoint. Its Bronze task reloads all CSVs and creates the checkpoint that subsequent delta loads reuse. Do not delete the checkpoint during ordinary delta operation. A failed full rebuild may leave partial results because resets and table writes are separate commits; rerun full after resolving its failure.

Silver trades append only files not already present by `source_file`. Silver summary uses `MERGE` by `symbol, data`, comparing source counts against `quantidade_trades`. Missing summary dates are automatically rebuilt from retained Silver trades. Same-count corrections and partial deletions within an already-seen Silver source file are not detected by these rules; these limitations are unchanged in this review.

SMA50/200 recompute dates affected by changed closing prices or missing Gold rows (D through D+49 / D+199); a value remains null until its complete calendar-day window exists. CVD recomputes from the first missing/changed daily delta, seeded by the preceding retained cumulative value. VWAP updates missing/changed daily volumes. Full rebuilds all four Gold targets. Decimal precision is explicit in Silver and Gold; compare cross-engine averages with an appropriate decimal tolerance.

`ORCHESTRATION.DQ_SUMMARY_BASELINE` is retained from the uploaded Databricks implementation. It records the last **successful summary validation** per symbol/load mode; it does not imply Gold succeeded and is never required before the first full load. It contains scope dates, validation timestamp and job run ID. It is an audit record, not the live retention boundary, so pruning does not rewrite it.

## Retention and directory structure

There is no Databricks `maintenance` directory. `orchestration/prune_366_days.py` runs only after all four delta Gold branches succeed. It removes complete ZIP/CSV archives and rows strictly older than UTC today minus 366 days, keeping the cutoff date and Auto Loader checkpoint. File removal and writes across Delta tables are not one transaction; retry the prune task if it fails partway. No `VACUUM` is run; physical Delta storage may retain old versions for time travel. Full has no automatic pruning task.

The active folders are `etl/silver_layer`, `etl/gold_layer`, `quality`, `orchestration`, `setup_init`, `setup_redeploy`, `pipeline_evaluation`, `docs` and `debug_sql` (now Python notebooks). Databricks also retains `historical_data` and the ingestion notebook; `src` contains shared implementations. dbt-only macros/profiles and Snowflake-only integration setup have no Databricks equivalent. This delivery contains no Snowflake pipeline.

## Evaluation

`pipeline_evaluation/data.py` replaces `data.sql`: first 10 rows for all eight requested tables plus exact counts. Raw-table counts and sorts can be expensive.

`pipeline_evaluation/costs.py` replaces `costs.sql`: active Delta snapshot sizes, Volume bytes including ZIP/CSV/checkpoints, summary date coverage, job usage/list-price estimates and job/task timelines. Configure the time window and currency under `databricks.evaluation`. It discovers the current workspace ID through the SDK; `evaluation.workspace_id` can override it. Job IDs and workspace ID jointly restrict attribution. Retractions/restatements retain their signed quantities; missing prices are reported rather than replaced with zero.

System tables require permissions and may be unavailable in Free Edition. Such reports print `UNAVAILABLE` with the cause. Sizes and costs are not Snowflake-credit equivalents: active Delta bytes exclude obsolete/time-travel files; list-price estimates exclude negotiated discounts, unattributed shared compute, external cloud infrastructure and storage invoices. Daily and per-run reports overlap and must not be summed together.

## Validation

Local PySpark tests cover ingestion date defaults, landing gaps/duplicates, task graph structure, summary deletion/backfill, count changes, full versus incremental indicators and missing Gold rows. Mocked deployment tests cover ID reuse, complete task reset, new job creation and rejection of ambiguous ownership. Databricks-specific Auto Loader, Unity Catalog DDL, Delta writes, billing access and live Jobs API deployment require a workspace smoke test; they were not executed against your account.

Run local tests with Python 3.11, Java 17, and `tests/requirements.txt`: `SPARK_LOCAL_IP=127.0.0.1 python -m pytest databricks/tests -q`.

References: [Jobs API](https://docs.databricks.com/api/jobs/v2/job), [task values](https://docs.databricks.com/gcp/en/jobs/task-values), [serverless streaming](https://docs.databricks.com/aws/en/compute/serverless/streaming), [job system tables](https://docs.databricks.com/aws/en/admin/system-tables/jobs), [billing prices](https://docs.databricks.com/aws/en/admin/system-tables/pricing).
