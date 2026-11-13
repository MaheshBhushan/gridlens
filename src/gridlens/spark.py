"""SparkSession factory: Databricks-provided session there, local Delta session elsewhere."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "gridlens") -> SparkSession:
    if on_databricks():
        # Serverless and classic compute both expose an active session with Delta built in.
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        return spark

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("GRIDLENS_SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.environ.get("GRIDLENS_DRIVER_MEMORY", "1g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    if warehouse := os.environ.get("GRIDLENS_WAREHOUSE_DIR"):
        builder = builder.config("spark.sql.warehouse.dir", warehouse)
    if ivy := os.environ.get("GRIDLENS_IVY_DIR"):
        builder = builder.config("spark.jars.ivy", ivy)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
