"""Create the reporting views from ``sql/views.sql`` against the lakehouse schema."""

from __future__ import annotations

from importlib import resources

from gridlens.forecast import FORECAST_TABLE, METRICS_TABLE
from gridlens.gold import DAILY_PEAK_TABLE, LOAD_PROFILE_TABLE
from gridlens.silver import DQ_REPORT_TABLE
from gridlens.storage import Lakehouse

VIEW_SOURCE_TABLES = [LOAD_PROFILE_TABLE, DAILY_PEAK_TABLE, FORECAST_TABLE, METRICS_TABLE, DQ_REPORT_TABLE]


def view_statements(schema: str) -> list[str]:
    text = resources.files("gridlens").joinpath("sql/views.sql").read_text()
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return [s.strip().replace("{schema}", schema) for s in body.split(";") if s.strip()]


def create_views(lake: Lakehouse) -> list[str]:
    lake.register_local_tables(VIEW_SOURCE_TABLES)
    created = []
    for stmt in view_statements(lake.sql_schema):
        lake.spark.sql(stmt)
        created.append(stmt.split()[4].split(".")[-1])
    return created
