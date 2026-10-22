# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold product performance
# MAGIC
# MAGIC Recompute product metrics from Silver, then MERGE the aggregate snapshot by date, category, and product_id.
# COMMAND ----------
# MAGIC %run /Workspace/Users/<your_email>/shopstream/notebooks/00_shared_config

# COMMAND ----------
# ============================================================
# GOLD LAYER — BUSINESS-READY AGGREGATES
# ============================================================
# gold_product_performance: "WHICH products drive the business?"
#
# This mart is only possible because we kept products + order_items
# through the pipeline. It joins:
#     silver_order_items (what was sold, how many, at what price)
#   + silver_products    (what category the product belongs to)
#   + silver_orders      (order date + status)
#
# Grain: one row per (order_date, category, product_id)
# Answered questions: top products by revenue, category trends,
# average selling price vs list price, basket composition.
# ============================================================

from pyspark.sql.functions import sum, count, avg, col, round as _round
from delta.tables import DeltaTable

# COMMAND ----------
gold_product_performance = (
    spark.table("silver_order_items")
    .join(spark.table("silver_orders"), "order_id", "left")
    .filter(~col("status").isin("cancelled", "refunded"))          # real sales only
    .join(spark.table("silver_products"), "product_id", "left")
    .groupBy("order_date", "category", "product_id")
    .agg(
        sum("quantity").alias("units_sold"),
        sum(col("quantity") * col("unit_price")).alias("item_revenue"),
        avg("unit_price").alias("avg_selling_price"),
        count("order_id").alias("times_ordered"),
    )
    # round money columns for clean dashboard display
    .withColumn("item_revenue", _round("item_revenue", 2))
    .withColumn("avg_selling_price", _round("avg_selling_price", 2))
)

# COMMAND ----------
if not spark.catalog.tableExists("gold_product_performance"):
    gold_product_performance.limit(0).write.format("delta").saveAsTable("gold_product_performance")

target = DeltaTable.forName(spark, "gold_product_performance")

(
    target.alias("t")
    .merge(
        gold_product_performance.alias("s"),
        "t.order_date <=> s.order_date "
        "AND t.category <=> s.category "
        "AND t.product_id <=> s.product_id",
    )
    .whenMatchedUpdate(
        set={
            "units_sold": "s.units_sold",
            "item_revenue": "s.item_revenue",
            "avg_selling_price": "s.avg_selling_price",
            "times_ordered": "s.times_ordered",
        }
    )
    .whenNotMatchedInsert(
        values={
            "order_date": "s.order_date",
            "category": "s.category",
            "product_id": "s.product_id",
            "units_sold": "s.units_sold",
            "item_revenue": "s.item_revenue",
            "avg_selling_price": "s.avg_selling_price",
            "times_ordered": "s.times_ordered",
        }
    )
    .whenNotMatchedBySourceDelete()
    .execute()
)

print("gold_product_performance merged successfully")
