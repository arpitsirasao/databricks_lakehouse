# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze raw landing
# MAGIC
# MAGIC Preserve each incoming file in Delta with run metadata. Initial files overwrite the baseline tables; incremental files append to separate raw tables.
# COMMAND ----------
# MAGIC %run /Workspace/Users/<your_email>/shopstream/notebooks/00_shared_config

# COMMAND ----------
# ============================================================
# BRONZE LAYER — RAW, APPEND-ONLY LANDING ZONE
# ============================================================
# ONE Bronze notebook for BOTH load modes. The `load_type` widget
# (a job parameter) decides what this task does:
#
#   initial     -> reads data/initial_load/*      -> bronze_customers,
#                  bronze_orders, bronze_products, bronze_order_items
#   incremental -> reads data/incremental_load/*  -> bronze_orders_incremental,
#                  bronze_order_items_incremental
#
# BRONZE RULES (why this layer exists):
#   1. Store data EXACTLY as it arrived — no cleaning, no casting, no filters.
#      If an upstream system sends garbage, we keep the garbage as evidence.
#   2. WRITE MODE BY RUN TYPE:
#        initial     -> OVERWRITE (idempotent backfill: re-running the first
#                       load can never create duplicates)
#        incremental -> APPEND    (each daily drop is preserved as raw
#                       evidence; duplicate rows are Silver's problem,
#                       resolved there by dedupe/MERGE)
#      Safe because incremental data lands in SEPARATE tables
#      (bronze_*_incremental) — overwriting the initial tables never
#      touches daily history.
#   3. Stamp every row with ingested_at, batch_date, source_file so every
#      record is traceable to the exact run and file that produced it.
# ============================================================

from pyspark.sql.functions import col, current_timestamp, lit

load_type = dbutils.widgets.get("load_type")
batch_date = dbutils.widgets.get("batch_date")

INITIAL_FILES = {
    "customers.csv": "bronze_customers",
    "orders.csv": "bronze_orders",
    "products.csv": "bronze_products",
    "order_items.csv": "bronze_order_items",
}

INCREMENTAL_FILES = {
    "orders_incremental.csv": "bronze_orders_incremental",
    "order_items_incremental.csv": "bronze_order_items_incremental",
}

# COMMAND ----------

def load_folder(folder, files, mode):
    for file_name, table_name in files.items():
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{folder}/{file_name}")
            .withColumn("ingested_at", current_timestamp())
            .withColumn("batch_date", lit(batch_date))
            .withColumn("source_file", col("_metadata.file_path"))
        )

        if mode == "overwrite":
            # overwriteSchema guards against inferSchema drift between runs
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(table_name)
        else:
            df.write.format("delta").mode("append").saveAsTable(table_name)

        print(f"loaded {file_name} -> {table_name} [mode={mode}] ({df.count()} rows)")

    # COMMAND ----------

if load_type == "initial":
    load_folder(f"{workspace_root}/data/initial_load", INITIAL_FILES, mode="overwrite")
elif load_type == "incremental":
    load_folder(f"{workspace_root}/data/incremental_load", INCREMENTAL_FILES, mode="append")
else:
    raise ValueError(f"Unknown load_type '{load_type}'. Use 'initial' or 'incremental'.")

print(f"Bronze load complete (load_type={load_type}, batch_date={batch_date})")
