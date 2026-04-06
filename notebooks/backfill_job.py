# Databricks notebook source
# MAGIC %md
# MAGIC # Backfill Job Entry Point
# MAGIC
# MAGIC This notebook is invoked once per date by the `for_each_task` fan-out.
# MAGIC It receives a single `run_date` and executes the pipeline logic for that date.
# MAGIC
# MAGIC This is a sample/template — replace the processing logic with your actual pipeline.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("env", "dev", "Environment (dev/staging/prod)")

# COMMAND ----------

run_date_str = dbutils.widgets.get("run_date").strip()
env = dbutils.widgets.get("env").strip()

if not run_date_str:
    raise ValueError("run_date is required. This notebook should be called via for_each_task.")

run_date = datetime.strptime(run_date_str, "%Y-%m-%d")
print(f"Processing backfill for date: {run_date_str}, env: {env}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Processing Logic
# MAGIC Replace this section with your actual pipeline steps.

# COMMAND ----------

# Example: Read source data for the target date
# source_df = spark.table(f"catalog.schema.source_table").filter(
#     F.col("event_date") == run_date_str
# )

# Example: Transform
# result_df = source_df.groupBy("category").agg(
#     F.sum("amount").alias("total_amount"),
#     F.count("*").alias("record_count"),
# )

# Example: Write with partition overwrite (MODIFY permission only, no MANAGE needed)
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# result_df.withColumn("run_date", F.lit(run_date_str)).write \
#     .mode("overwrite") \
#     .insertInto(f"catalog.schema.target_table")

# For Delta tables, use replaceWhere for surgical overwrites:
# result_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("replaceWhere", f"run_date = '{run_date_str}'") \
#     .saveAsTable(f"catalog.schema.target_table")

# COMMAND ----------

# Placeholder: simulate processing
print(f"Backfill complete for {run_date_str}")
