# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver customers
# MAGIC
# MAGIC Normalize customer emails and signup dates, keep the latest row per customer, and write the curated snapshot.
# COMMAND ----------
# MAGIC %run /Workspace/Users/<your_email>/shopstream/notebooks/00_shared_config

# COMMAND ----------
# ============================================================
# SILVER LAYER — CLEANED, CONFORMED, TRUSTED
# ============================================================
# Customers: read from bronze_customers (NEVER from raw CSVs).
#
# SILVER RULES (why this layer exists):
#   1. Read only from Bronze tables — the raw truth stays untouched.
#   2. Fix messy data ONCE here, so Gold and dashboards never see dirt:
#        - mixed-case emails            -> lower(trim())
#        - mixed date formats           -> coalesce(to_date(...), to_date(...))
#        - duplicate customer records   -> keep latest by ingested_at
#   3. Deduplicate deterministically (latest record wins), so re-runs
#      always produce the same trusted result.
# ============================================================

from pyspark.sql.functions import col, lower, trim, row_number, to_date, try_to_date, coalesce
from pyspark.sql.window import Window

# Customer CSVs mix ISO (2022-03-15) and US (01/15/2023) date formats.
# coalesce tries ISO first; if NULL, tries the US format.
signup_date_clean = coalesce(
    try_to_date(col("signup_date"), "yyyy-MM-dd"),
    try_to_date(col("signup_date"), "MM/dd/yyyy"),
)

window_spec = Window.partitionBy("customer_id").orderBy(col("ingested_at").desc())

# COMMAND ----------
bronze_customers = spark.table("bronze_customers")

silver_customers = (
    bronze_customers
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("signup_date", signup_date_clean)
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)                    # keep latest record per customer
    .drop("rn")
    .filter(col("customer_id").isNotNull())
)

bronze_count = bronze_customers.count()
silver_count = silver_customers.count()
log_count_change(
    "silver_customers", bronze_count, silver_count
)

# COMMAND ----------
silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")

print(f"silver_customers written ({silver_customers.count()} rows)")