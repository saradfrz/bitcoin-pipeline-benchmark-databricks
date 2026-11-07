# Quality checks

The root task validates one snapshot of the CSV landing directory. It rejects missing, empty or invalid daily archive names, duplicated dates and gaps between the earliest and latest dates. It publishes `lo`, `hi` and `summary_hi` using Databricks task values. Full resets run only after this check succeeds.

`DQ_AFTER_SUMMARY` requires exactly one BTCUSDT summary row for every day in the root's scope. Full uses `lo..hi`; delta uses `lo..yesterday UTC`. Four Gold branches depend on this successful check. A missing summary row can be reconstructed by the preceding summary model before the check runs. To demonstrate failure itself, inspect or validate immediately after deletion and before running delta.

`DQ_SUMMARY_BASELINE` records the last successful summary check per load mode. It is not consulted before creating a summary and does not require bootstrapping. It is not evidence that downstream Gold or pruning succeeded.

These checks mirror the Snowflake project's coverage checks, not a comprehensive record-level data contract. In particular, they do not verify every CSV trade field or detect same-count corrections.
