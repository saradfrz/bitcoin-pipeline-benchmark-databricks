select count(*) from btc_trading.binance_bronze.btcusdt_trades_bronze;
select * from btc_trading.binance_bronze.btcusdt_trades_bronze limit 10;
-- truncate table btc_trading.binance_bronze.btcusdt_trades_bronze;

SELECT count(*) from BTC_TRADING.BINANCE_BRONZE.BTCUSDT_TRADES_BRONZE;

SELECT
    DATE_TRUNC('month', ingested_at) AS ingestion_month,
    COUNT(*) AS total_rows
FROM btc_trading.binance_bronze.btcusdt_trades_bronze
GROUP BY DATE_TRUNC('month', ingested_at)
ORDER BY ingestion_month;

SET TIME ZONE 'UTC';

SELECT
    *
FROM btc_trading.binance_bronze.btcusdt_trades_bronze
order by trade_id  --= 6803172381;


SELECT * from btc_trading.binance_bronze.btcusdt_trades_bronze where trade_id = '%6803172381%'