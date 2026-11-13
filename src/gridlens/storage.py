"""Where Delta tables live: a local directory, or Unity Catalog on Databricks.

All pipeline code reads and writes through :class:`Lakehouse`, so the same
transformations run against ``./lakehouse/<table>`` locally and against
``<catalog>.<schema>.<table>`` in a Databricks workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

LOCAL_SCHEMA = "gridlens"


@dataclass
class Lakehouse:
    spark: SparkSession
    base_path: str | None = None
    catalog: str | None = None
    schema: str = LOCAL_SCHEMA

    def __post_init__(self) -> None:
        if (self.base_path is None) == (self.catalog is None):
            raise ValueError("set exactly one of base_path (local) or catalog (Unity Catalog)")
        if self.base_path is not None:
            self.base_path = str(Path(self.base_path).resolve())

    @property
    def is_catalog(self) -> bool:
        return self.catalog is not None

    @property
    def sql_schema(self) -> str:
        """Schema name to use in SQL view definitions."""
        return f"`{self.catalog}`.`{self.schema}`" if self.is_catalog else f"`{self.schema}`"

    def path(self, name: str) -> str:
        assert self.base_path is not None
        return f"{self.base_path}/{name}"

    def table_ref(self, name: str) -> str:
        if self.is_catalog:
            return f"{self.sql_schema}.`{name}`"
        return f"delta.`{self.path(name)}`"

    def raw_dir(self) -> str:
        if self.is_catalog:
            return f"/Volumes/{self.catalog}/{self.schema}/raw"
        return str(Path(self.base_path).parent / "data" / "raw")

    def ensure_schema(self, with_raw_volume: bool = False) -> None:
        if self.is_catalog:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.sql_schema}")
            if with_raw_volume:
                self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {self.sql_schema}.`raw`")
        else:
            Path(self.base_path).mkdir(parents=True, exist_ok=True)

    def write(self, df: DataFrame, name: str) -> None:
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if self.is_catalog:
            writer.saveAsTable(f"{self.catalog}.{self.schema}.{name}")
        else:
            writer.save(self.path(name))

    def read(self, name: str) -> DataFrame:
        if self.is_catalog:
            return self.spark.table(f"{self.catalog}.{self.schema}.{name}")
        return self.spark.read.format("delta").load(self.path(name))

    def register_local_tables(self, names: list[str]) -> None:
        """Expose path-based Delta tables under ``gridlens.<name>`` so the SQL views resolve locally."""
        if self.is_catalog:
            return
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.sql_schema}")
        for name in names:
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {self.sql_schema}.`{name}` USING DELTA LOCATION '{self.path(name)}'"
            )
