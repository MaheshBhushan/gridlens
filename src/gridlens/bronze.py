"""Bronze: land the raw LCL CSV files as-is (all strings) plus ingestion metadata."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from gridlens.storage import Lakehouse

BRONZE_TABLE = "bronze_readings"

# Source header: LCLid,stdorToU,DateTime,KWH/hh (per half hour)
# Delta forbids spaces and slashes in column names without column mapping, so the raw
# columns are renamed positionally; values stay untouched strings for the silver layer.
RAW_SCHEMA = StructType(
    [
        StructField("lcl_id", StringType()),
        StructField("std_or_tou", StringType()),
        StructField("datetime_raw", StringType()),
        StructField("kwh_raw", StringType()),
    ]
)


def read_raw_csv(spark: SparkSession, source: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        # Only *.csv: the raw directory or Unity Catalog volume may hold other files.
        .option("pathGlobFilter", "*.csv")
        .schema(RAW_SCHEMA)
        .csv(source)
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("ingested_at", F.current_timestamp())
    )


def build_bronze(lake: Lakehouse, source: str) -> int:
    df = read_raw_csv(lake.spark, source)
    lake.write(df, BRONZE_TABLE)
    return lake.read(BRONZE_TABLE).count()
