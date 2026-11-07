-- Restart the project
DROP VOLUME IF EXISTS btc_trading.binance_bronze.landing_zone;

DROP SCHEMA IF EXISTS btc_trading.binance_gold CASCADE;
DROP SCHEMA IF EXISTS btc_trading.binance_silver CASCADE;
DROP SCHEMA IF EXISTS btc_trading.binance_bronze CASCADE;

DROP CATALOG IF EXISTS btc_trading;


-- Create the project
CREATE CATALOG IF NOT EXISTS btc_trading;

CREATE SCHEMA IF NOT EXISTS btc_trading.binance_bronze;
CREATE SCHEMA IF NOT EXISTS btc_trading.binance_silver;
CREATE SCHEMA IF NOT EXISTS btc_trading.binance_gold;

CREATE VOLUME IF NOT EXISTS btc_trading.binance_bronze.landing_zone;