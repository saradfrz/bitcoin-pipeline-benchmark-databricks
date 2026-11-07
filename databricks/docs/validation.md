# Validation results

Six tests passed under local PySpark 3.5.3, Java 17 and Python 3.11:

- Runtime ingestion date defaults, custom bounds and invalid landing coverage.
- Four YAML definitions, paths, dependency order and exact processing task counts.
- Daily OHLC/trade-count aggregation and reconstruction after a summary deletion.
- SMA50, SMA200, CVD and VWAP incremental results versus full rebuilds, including missing Gold dates.
- Jobs API reset payload preserves the existing ID and supplies the complete task graph; creation handles missing jobs (mocked API).
- All deployment targets are validated before mutation; unowned exact-name collisions fail (mocked API).

All Python files parsed and YAML files loaded successfully. Snowflake files were byte-compared with the uploaded project and remain unchanged. No Databricks `.ipynb` files or maintenance directory remain.

Not run here: live Databricks Auto Loader, Unity Catalog setup, Delta table writes, Jobs API deployment, Binance bulk downloads, or system billing queries. These require the user's workspace and permissions. Use a small completed UTC date range with ingestion_full/full for the initial workspace smoke test. The delta DQ scope still requires coverage through yesterday UTC.
