"""Load headerless Binance daily trade CSVs directly into Delta Bronze."""

from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


TRADE_COLUMNS = (
    "trade_id", "price", "quantity", "quote_quantity", "trade_time_us",
    "is_buyer_maker", "is_best_match",
)


class BinanceBronzeLoader:
    """Run one writer at a time, with a stable checkpoint for the target table."""

    def __init__(self, spark, volume, table, symbol="BTCUSDT"):
        self.spark = spark
        self.volume = Path(volume)
        self.table = table
        self.symbol = symbol.upper()
        self.csv_dir = self.volume / "unzipped"
        self.checkpoint = self.volume / "_checkpoints" / f"{self.symbol.lower()}_csv_bronze_v1"

    def run(self):
        schema = StructType([StructField(name, StringType()) for name in TRADE_COLUMNS])
        trades = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "false")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.maxFilesPerTrigger", 5)
            .option("pathGlobFilter", f"{self.symbol}-trades-*.csv")
            .schema(schema)
            .load(str(self.csv_dir))
            .select(*TRADE_COLUMNS, F.lit(self.symbol).alias("symbol"),
                    F.col("_metadata.file_path").alias("source_file"),
                    F.current_timestamp().alias("ingested_at"))
        )
        query = (
            trades.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", str(self.checkpoint))
            .trigger(availableNow=True)
            .toTable(self.table)
        )
        query.awaitTermination()
