# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
from charter_utils import load_config, add_run_metadata, deduplicate_by_key

# COMMAND ----------
runDate = dbutils.widgets.get("runDate")
outputEnvironment = dbutils.widgets.get("outputEnvironment")
overwrite = dbutils.widgets.get("overwrite")
appConfig = dbutils.widgets.get("appConfig")

print(f"{runDate=}\n{outputEnvironment=}\n{overwrite=}\n{appConfig=}")
# COMMAND ----------

configPath = dbutils.widgets.get("configPath")
print(f'configPath: {configPath}')
env = dbutils.widgets.get("env")
print(f'env: {env}')
workflow_runDate = dbutils.widgets.get("workflow_runDate")
print(f"{workflow_runDate}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo: using charter_utils wheel library

# COMMAND ----------

config = load_config(spark, configPath)
print(f"Loaded config: {config}")

# COMMAND ----------

sample_df = spark.createDataFrame(
    [("device_1", "2024-01-01", 100), ("device_1", "2024-01-02", 200), ("device_2", "2024-01-01", 50)],
    ["device_id", "event_date", "metric"],
)

enriched_df = add_run_metadata(sample_df, runDate, outputEnvironment)
enriched_df.show()

# COMMAND ----------

deduped_df = deduplicate_by_key(sample_df, ["device_id"], "event_date")
deduped_df.show()