# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold daily sales
# MAGIC
# MAGIC Recompute daily sales from Silver, then MERGE the aggregate snapshot by date, country, and membership tier.
# COMMAND ----------
# MAGIC %run /Workspace/Users/<your_email>/shopstream/notebooks/00_shared_config

# COMMAND ----------
# ============================================================
# GOLD LAYER — BUSINESS-READY AGGREGATES
# ============================================================
# gold_daily_sales: "How is the business doing, per day/country/tier?"
#
# GOLD RULES (why this layer exists):
#   1. Read only from Silver — never from Bronze or raw files.
#   2. Answer a BUSINESS question, not a storage question. Every column
#      here maps to something an executive wants to see on a dashboard.
#   3. Only REAL revenue counts: cancelled and refunded orders are
#      excluded. (status filter added here — the single most common
#      mistake in first pipelines is counting refunds as revenue.)
#
# Grain: one row per (order_date, country, membership_tier)
# ============================================================

from pyspark.sql.functions import sum, count, avg, expr, col
from delta.tables import DeltaTable

# COMMAND ----------
gold_daily_sales = (
    spark.table("silver_orders")
    .filter(~col("status").isin("cancelled", "refunded"))   # real revenue only
    .join(spark.table("silver_customers"), "customer_id", "left")  # keep orphan orders visible
    .groupBy("order_date", "country", "membership_tier")
    .agg(
        count("order_id").alias("total_transactions"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value"),
        expr("count(distinct customer_id)").alias("active_buyers"),
    )
)

# COMMAND ----------
if not spark.catalog.tableExists("gold_daily_sales"):
    gold_daily_sales.limit(0).write.format("delta").saveAsTable("gold_daily_sales")

target = DeltaTable.forName(spark, "gold_daily_sales")

(
    target.alias("t")
    .merge(
        gold_daily_sales.alias("s"),
        "t.order_date <=> s.order_date "
        "AND t.country <=> s.country "
        "AND t.membership_tier <=> s.membership_tier",
    )
    .whenMatchedUpdate(
        set={
            "total_transactions": "s.total_transactions",
            "total_revenue": "s.total_revenue",
            "avg_order_value": "s.avg_order_value",
            "active_buyers": "s.active_buyers",
        }
    )
    .whenNotMatchedInsert(
        values={
            "order_date": "s.order_date",
            "country": "s.country",
            "membership_tier": "s.membership_tier",
            "total_transactions": "s.total_transactions",
            "total_revenue": "s.total_revenue",
            "avg_order_value": "s.avg_order_value",
            "active_buyers": "s.active_buyers",
        }
    )
    .whenNotMatchedBySourceDelete()
    .execute()
)

print("gold_daily_sales merged successfully")
