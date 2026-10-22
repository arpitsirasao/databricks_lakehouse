# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver orders
# MAGIC
# MAGIC Build the historical order table on an initial run; on incremental runs, normalize and MERGE the new order batch by order_id.
# COMMAND ----------
# MAGIC %run /Workspace/Users/<your_email>/shopstream/notebooks/00_shared_config

# COMMAND ----------
# ============================================================
# SILVER LAYER — CLEANED, CONFORMED, TRUSTED
# ============================================================
# Orders: handles BOTH load modes, mirroring the single Bronze notebook.
#
#   initial     -> dedupe the full history in bronze_orders,
#                  overwrite silver_orders (one-time rebuild)
#   incremental -> Delta MERGE today's bronze_orders_incremental
#                  into silver_orders (update changed, insert new)
#
# SILVER RULES:
#   1. Read only from Bronze tables.
#   2. Dedupe the source batch BEFORE the MERGE (two rows with the same
#      order_id in one file would otherwise crash the MERGE).
#   3. Normalize dates ONCE: historical CSV uses ISO dates, but the
#      incremental drops carry Excel serial numbers (45566 = 2024-09-21).
#   4. MERGE = upsert in a single ACID transaction: no duplicates,
#      no overwrites, and the delta history records every change.
# ============================================================

from pyspark.sql.functions import col, row_number, to_date, expr
from pyspark.sql.window import Window
from delta.tables import DeltaTable

load_type = dbutils.widgets.get("load_type")


def dedupe_latest(df, key):
    window_spec = Window.partitionBy(key).orderBy(col("ingested_at").desc())
    return (
        df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def normalize_order_date(df):
    # Convert 5-digit Excel serials to real dates; pass ISO strings through.
    return df.withColumn(
        "order_date",
        expr(
            "CASE WHEN rlike(cast(order_date as string), '^[0-9]{5}$') "
            "THEN date_add(date'1899-12-30', cast(order_date as int)) "
            "ELSE to_date(order_date) END"
        ),
    )

# COMMAND ----------

if load_type == "initial":
    bronze_orders = spark.table("bronze_orders")
    silver_orders = (
        dedupe_latest(bronze_orders, "order_id")
        .withColumn("order_date", to_date(col("order_date")))
        .filter(col("order_id").isNotNull())
    )
    log_count_change(
        "silver_orders initial", bronze_orders.count(), silver_orders.count()
    )
    silver_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
    print(f"Initial silver_orders written ({silver_orders.count()} rows)")

elif load_type == "incremental":
    bronze_batch = spark.table("bronze_orders_incremental")
    source_df = normalize_order_date(
        dedupe_latest(bronze_batch, "order_id")
    ).filter(col("order_id").isNotNull())

    source_count = source_df.count()
    log_count_change(
        "silver_orders incremental batch",
        bronze_batch.count(),
        source_count,
        before_label="bronze_batch",
        after_label="silver_batch",
    )
    target_before = spark.table("silver_orders").count() if spark.catalog.tableExists("silver_orders") else 0

    if not spark.catalog.tableExists("silver_orders"):
        source_df.limit(0).write.format("delta").saveAsTable("silver_orders")

    target = DeltaTable.forName(spark, "silver_orders")

    (
        target.alias("t")
        .merge(
            source_df.alias("s"),
            "t.order_id = s.order_id",
        )
        .whenMatchedUpdate(
            set={
                "status": "s.status",
                "total_amount": "s.total_amount",
                "order_date": "s.order_date",
            }
        )
        .whenNotMatchedInsert(
            values={
                "order_id": "s.order_id",
                "customer_id": "s.customer_id",
                "order_date": "s.order_date",
                "status": "s.status",
                "total_amount": "s.total_amount",
            }
        )
        .execute()
    )

    target_after = spark.table("silver_orders").count()
    log_count_change(
        "silver_orders incremental target",
        target_before,
        target_after,
        before_label="target_before",
        after_label="target_after",
        change_label="net_change",
    )
    print("Incremental MERGE into silver_orders complete")

else:
    raise ValueError(f"Unknown load_type '{load_type}'. Use 'initial' or 'incremental'.")
