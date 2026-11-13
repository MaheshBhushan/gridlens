# Databricks notebook source
# MAGIC %md
# MAGIC # GridLens report
# MAGIC Final task of the `gridlens-pipeline` job. Reads the reporting views built by the
# MAGIC `sql_views` task, displays them and fails the run if the gold layer is incomplete.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")  # noqa: F821 - provided by Databricks
dbutils.widgets.text("schema", "gridlens")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
fq = f"`{catalog}`.`{schema}`"

# COMMAND ----------

# MAGIC %md ## Data quality

# COMMAND ----------

dq = spark.sql(f"SELECT * FROM {fq}.v_data_quality ORDER BY rows DESC")  # noqa: F821
display(dq)  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Forecast accuracy (held-out)

# COMMAND ----------

acc = spark.sql(f"SELECT * FROM {fq}.v_forecast_accuracy ORDER BY split, model")  # noqa: F821
display(acc)  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Top peak-demand days and weekday winter load profile

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {fq}.v_peak_demand WHERE peak_rank_overall <= 10 ORDER BY peak_rank_overall"))  # noqa: F821
display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"SELECT tariff, slot_label, mean_kw_per_household FROM {fq}.v_load_profile "
        "WHERE season = 'winter' AND day_type = 'weekday' ORDER BY tariff, slot"
    )
)

# COMMAND ----------

checks = {
    "silver rows": spark.sql(f"SELECT rows FROM {fq}.v_data_quality WHERE rule = 'TOTAL_PASSED'").first()[0],  # noqa: F821
    "load profile rows": spark.table(f"{catalog}.{schema}.v_load_profile").count(),  # noqa: F821
    "complete peak days": spark.table(f"{catalog}.{schema}.v_peak_demand").count(),  # noqa: F821
    "held-out forecast rows": spark.sql(  # noqa: F821
        f"SELECT count(*) FROM {fq}.v_consumption_forecast WHERE split = 'test'"
    ).first()[0],
    "future forecast rows": spark.sql(  # noqa: F821
        f"SELECT count(*) FROM {fq}.v_consumption_forecast WHERE split = 'future'"
    ).first()[0],
}
print(checks)
empty = [k for k, v in checks.items() if not v]
assert not empty, f"empty gold outputs: {empty}"
