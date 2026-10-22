# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver order items
# MAGIC
# MAGIC Validate line-item identifiers, quantities, and prices. Initial runs build the baseline; incremental runs MERGE by item_id.
# COMMAND ----------
# MAGIC %run /Workspace/Users/<your_email>/shopstream/notebooks/00_shared_config

# COMMAND ----------
# ============================================================
# SILVER LAYER — CLEANED, CONFORMED, TRUSTED
# ============================================================
# Order items (line items): handles BOTH load modes.
#
#   initial     -> clean + dedupe bronze_order_items, overwrite silver_order_items
#   incremental -> Delta MERGE today's bronze_order_items_incremental
#                  into silver_order_items (keyed on item_id)
#
# WHY THIS TABLE MATTERS:
#   Orders only tell you the total bill. Line items tell you WHAT was sold,
#   HOW MUCH of it, and AT WHAT PRICE — the raw material for product-level
#   analytics (top products, category revenue, basket size).
#
# Data quality enforced here:
#   - quantity must be > 0 (a 0 or negative quantity is nonsense)
#   - unit_price must be >= 0
#   - rows missing item_id / order_id / product_id are rejected
# ============================================================

from pyspark.sql.functions import col, row_number
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


def clean_items(df):
    return (
        dedupe_latest(df, "item_id")
        .filter(col("item_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") >= 0)
    )

# COMMAND ----------

if load_type == "initial":
    bronze_order_items = spark.table("bronze_order_items")
    silver_order_items = clean_items(bronze_order_items)
    log_count_change(
        "silver_order_items initial",
        bronze_order_items.count(),
        silver_order_items.count(),
    )
    silver_order_items.write.format("delta").mode("overwrite").saveAsTable("silver_order_items")
    print(f"Initial silver_order_items written ({silver_order_items.count()} rows)")

elif load_type == "incremental":
    bronze_batch = spark.table("bronze_order_items_incremental")
    source_df = clean_items(bronze_batch)
    source_count = source_df.count()
    log_count_change(
        "silver_order_items incremental batch",
        bronze_batch.count(),
        source_count,
        before_label="bronze_batch",
        after_label="silver_batch",
    )
    target_before = spark.table("silver_order_items").count() if spark.catalog.tableExists("silver_order_items") else 0

    if not spark.catalog.tableExists("silver_order_items"):
        source_df.limit(0).write.format("delta").saveAsTable("silver_order_items")

    target = DeltaTable.forName(spark, "silver_order_items")

    (
        target.alias("t")
        .merge(
            source_df.alias("s"),
            "t.item_id = s.item_id",
        )
        .whenMatchedUpdate(
            set={
                "quantity": "s.quantity",
                "unit_price": "s.unit_price",
            }
        )
        .whenNotMatchedInsert(
            values={
                "item_id": "s.item_id",
                "order_id": "s.order_id",
                "product_id": "s.product_id",
                "quantity": "s.quantity",
                "unit_price": "s.unit_price",
            }
        )
        .execute()
    )

    target_after = spark.table("silver_order_items").count()
    log_count_change(
        "silver_order_items incremental target",
        target_before,
        target_after,
        before_label="target_before",
        after_label="target_after",
        change_label="net_change",
    )
    print("Incremental MERGE into silver_order_items complete")

else:
    raise ValueError(f"Unknown load_type '{load_type}'. Use 'initial' or 'incremental'.")
