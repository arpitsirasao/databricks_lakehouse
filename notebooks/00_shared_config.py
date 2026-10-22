# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Shared notebook configuration
# MAGIC
# MAGIC Define the job widgets once, set the active Unity Catalog context, and expose the shared row-count logger to layer notebooks.
# COMMAND ----------

# Replace <your_email> with your actual Databricks email when you create the workspace.
dbutils.widgets.text("workspace_root", "/Workspace/Users/<your_email>/shopstream")
dbutils.widgets.dropdown("load_type", "initial", ["initial", "incremental"])
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "shopstream")
dbutils.widgets.text("batch_date", "")

dbutils.widgets.text("source_dataset", "orders")

# COMMAND ----------
workspace_root = dbutils.widgets.get("workspace_root")
load_type = dbutils.widgets.get("load_type")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
batch_date = dbutils.widgets.get("batch_date")
source_dataset = dbutils.widgets.get("source_dataset")

initial_path = f"{workspace_root}/data/initial_load"
incremental_path = f"{workspace_root}/data/incremental_load"
source_dir = initial_path if load_type == "initial" else incremental_path

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"workspace_root={workspace_root}")
print(f"load_type={load_type}")
print(f"catalog={catalog}")
print(f"schema={schema}")
print(f"batch_date={batch_date}")
print(f"source_dir={source_dir}")

# COMMAND ----------
def log_count_change(label, before_count, after_count, before_label="bronze", after_label="silver", change_label="filtered_by"):
	change = before_count - after_count
	print(
		f"{label}: {before_label}={before_count} -> {after_label}={after_count} | {change_label}={change}"
	)
