# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver products
# MAGIC
# MAGIC Deduplicate and validate the product snapshot, then MERGE catalog rows by product_id.
# COMMAND ----------
# MAGIC %run /Workspace/Users/<your_email>/shopstream/notebooks/00_shared_config

# COMMAND ----------
# ============================================================
# SILVER LAYER — CLEANED, CONFORMED, TRUSTED
# ============================================================
# Products: reference data. Products rarely change, so this notebook
# treats both modes the same way — MERGE the catalog on product_id
# (new products get inserted; price/name corrections get updated).
#
# WHY MERGE (not overwrite): if the source ever corrects a price,
# MERGE updates it in place while keeping the table's delta history —
# and it is idempotent, so accidental double-runs are harmless.
# ============================================================

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def dedupe_latest(df, key):
    window_spec = Window.partitionBy(key).orderBy(col("ingested_at").desc())
    return (
        df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

# COMMAND ----------

bronze_products = spark.table("bronze_products")
source_df = (
    dedupe_latest(bronze_products, "product_id")
    .filter(col("product_id").isNotNull())
    .filter(col("price") >= 0)
)

log_count_change(
    "silver_products", bronze_products.count(), source_df.count()
)

if not spark.catalog.tableExists("silver_products"):
    source_df.limit(0).write.format("delta").saveAsTable("silver_products")

target = DeltaTable.forName(spark, "silver_products")

# COMMAND ----------
(
    target.alias("t")
    .merge(
        source_df.alias("s"),
        "t.product_id = s.product_id",
    )
    .whenMatchedUpdate(
        set={
            "name": "s.name",
            "category": "s.category",
            "price": "s.price",
        }
    )
    .whenNotMatchedInsert(
        values={
            "product_id": "s.product_id",
            "name": "s.name",
            "category": "s.category",
            "price": "s.price",
        }
    )
    .execute()
)

print("silver_products upserted successfully")
