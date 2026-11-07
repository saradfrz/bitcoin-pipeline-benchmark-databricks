# Databricks notebook source
# MAGIC %md
# MAGIC # Load BTCUSDT Silver trades
# MAGIC Run after Bronze completes. Use one writer across both jobs. Bronze must commit complete, immutable source files with stable, nonempty source_file paths. Silver uses its committed source_file values as its processed-file inventory. If Bronze is rebuilt or a file is corrected under the same path, run setup_full.
# MAGIC
# MAGIC setup_full replaces all Silver trades from the current Bronze snapshot in one Delta transaction. delta appends all rows for files missing from Silver. Both modes preserve duplicate trades and expose malformed fields as null for the downstream quality task.

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "delta", ["delta", "setup_full"])
mode = dbutils.widgets.get("load_mode")
if mode not in {"delta", "setup_full"}:
    raise ValueError(f"Unsupported load_mode: {mode}")

bronze = "btc_trading.binance_bronze.btcusdt_trades_bronze"
silver = "btc_trading.binance_silver.btcusdt_trades"
spark.sql("SET TIME ZONE 'UTC'")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS btc_trading.binance_silver")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver} (
        trade_id BIGINT,
        price DECIMAL(38,8),
        quantity DECIMAL(38,8),
        quote_quantity DECIMAL(38,8),
        trade_time_us BIGINT,
        trade_timestamp_utc TIMESTAMP,
        is_buyer_maker BOOLEAN,
        is_best_match BOOLEAN,
        symbol STRING,
        source_file STRING,
        ingested_at TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# Pin the input so the metadata check and load use the same Bronze rows.
version = spark.sql(f"DESCRIBE HISTORY {bronze} LIMIT 1").first()["version"]
snapshot = f"{bronze} VERSION AS OF {int(version)}"
if spark.sql(f"""
    SELECT 1 FROM {snapshot}
    WHERE source_file IS NULL OR trim(source_file) = ''
    LIMIT 1
""").take(1):
    raise ValueError("Bronze contains a missing source_file. Repair file lineage before loading Silver.")

# COMMAND ----------

input_query = f"SELECT b.* FROM {snapshot} AS b"
if mode == "delta":
    input_query += f"""
        LEFT ANTI JOIN (SELECT DISTINCT source_file FROM {silver}) AS loaded
        ON b.source_file = loaded.source_file
    """

write_command = "INSERT OVERWRITE TABLE" if mode == "setup_full" else "INSERT INTO"
spark.sql(f"""
    {write_command} {silver} BY NAME
    WITH pending AS ({input_query}),
    typed AS (
        SELECT
            try_cast(trim(trade_id) AS BIGINT) AS trade_id,
            try_cast(trim(price) AS DECIMAL(38,8)) AS price,
            try_cast(trim(quantity) AS DECIMAL(38,8)) AS quantity,
            try_cast(trim(quote_quantity) AS DECIMAL(38,8)) AS quote_quantity,
            try_cast(trim(trade_time_us) AS BIGINT) AS trade_time_us,
            CASE lower(trim(is_buyer_maker))
                WHEN 'true' THEN true WHEN 'false' THEN false
            END AS is_buyer_maker,
            CASE lower(trim(is_best_match))
                WHEN 'true' THEN true WHEN 'false' THEN false
            END AS is_best_match,
            trim(symbol) AS symbol,
            source_file,
            ingested_at
        FROM pending
    )
    SELECT typed.*,
           CASE WHEN trade_time_us BETWEEN -62135596800000000 AND 253402300799999999
                THEN timestamp_micros(trade_time_us)
           END AS trade_timestamp_utc
    FROM typed
""").collect()
print(f"Silver load complete: mode={mode}, Bronze version={version}, target={silver}")