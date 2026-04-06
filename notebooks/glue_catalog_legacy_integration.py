# Databricks notebook source
# MAGIC %md
# MAGIC # Reading & Writing Glue Catalog Tables (Legacy Glue Integration)
# MAGIC
# MAGIC This notebook demonstrates how to read from and write to AWS Glue Data Catalog tables
# MAGIC when Glue is configured as the **external metastore** (legacy integration).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - The Databricks workspace must have the legacy Glue metastore configured
# MAGIC   (via `spark.hadoop.hive.metastore.client.factory.class` set to
# MAGIC   `com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory`)
# MAGIC - IAM role or instance profile with `glue:*` permissions attached to the cluster
# MAGIC - The target Glue database and S3 bucket must already exist (or you have permissions to create them)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Setup

# COMMAND ----------

# Glue database and table names
GLUE_DATABASE = "my_glue_database"
TARGET_TABLE = "sample_orders"
S3_LOCATION = "s3://my-bucket/glue-tables/sample_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify Glue Metastore Connectivity

# COMMAND ----------

# Confirm the metastore is pointing to Glue
metastore_class = spark.conf.get(
    "spark.hadoop.hive.metastore.client.factory.class", "not set"
)
print(f"Metastore factory class: {metastore_class}")
assert "AWSGlueDataCatalog" in metastore_class, (
    "This cluster is NOT configured with the legacy Glue metastore. "
    "Please attach a cluster that has the Glue Data Catalog enabled."
)

# COMMAND ----------

# List databases visible through Glue
display(spark.sql("SHOW DATABASES"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create a Glue Database (if needed)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GLUE_DATABASE}")
spark.sql(f"DESCRIBE DATABASE {GLUE_DATABASE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Data to a Glue Catalog Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Create sample data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

schema = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
    ]
)

data = [
    (1, "Alice", "Widget A", 10, 25.50),
    (2, "Bob", "Widget B", 5, 45.00),
    (3, "Charlie", "Widget A", 8, 25.50),
    (4, "Diana", "Widget C", 2, 120.00),
    (5, "Eve", "Widget B", 15, 45.00),
]

df = spark.createDataFrame(data, schema).withColumn(
    "order_date", F.current_timestamp()
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Write as a managed Parquet table registered in Glue

# COMMAND ----------

# Option 1: Managed table (Glue controls the storage location)
(
    df.write
    .mode("overwrite")
    .saveAsTable(f"{GLUE_DATABASE}.{TARGET_TABLE}_managed")
)

print(f"Managed table created: {GLUE_DATABASE}.{TARGET_TABLE}_managed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Write as an external table at a specific S3 location

# COMMAND ----------

# Option 2: External table pointing to a specific S3 path
(
    df.write
    .mode("overwrite")
    .option("path", S3_LOCATION)
    .saveAsTable(f"{GLUE_DATABASE}.{TARGET_TABLE}_external")
)

print(f"External table created: {GLUE_DATABASE}.{TARGET_TABLE}_external")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d. Write as a Delta table in Glue

# COMMAND ----------

DELTA_S3_LOCATION = "s3://my-bucket/glue-tables/sample_orders_delta"

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("path", DELTA_S3_LOCATION)
    .saveAsTable(f"{GLUE_DATABASE}.{TARGET_TABLE}_delta")
)

print(f"Delta table created: {GLUE_DATABASE}.{TARGET_TABLE}_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read Data from Glue Catalog Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Read using `spark.table()`

# COMMAND ----------

df_managed = spark.table(f"{GLUE_DATABASE}.{TARGET_TABLE}_managed")
display(df_managed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Read using Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM my_glue_database.sample_orders_external ORDER BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Read the Delta table and inspect history

# COMMAND ----------

df_delta = spark.table(f"{GLUE_DATABASE}.{TARGET_TABLE}_delta")
display(df_delta)

# COMMAND ----------

# Delta-specific: view table history (only works for Delta format)
display(spark.sql(f"DESCRIBE HISTORY {GLUE_DATABASE}.{TARGET_TABLE}_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Append & Upsert Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Append new rows

# COMMAND ----------

new_data = [
    (6, "Frank", "Widget A", 3, 25.50),
    (7, "Grace", "Widget C", 1, 120.00),
]

df_new = spark.createDataFrame(new_data, schema).withColumn(
    "order_date", F.current_timestamp()
)

# Append to the Delta table
df_new.write.format("delta").mode("append").saveAsTable(
    f"{GLUE_DATABASE}.{TARGET_TABLE}_delta"
)

print("Rows appended.")
display(spark.table(f"{GLUE_DATABASE}.{TARGET_TABLE}_delta").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Merge / Upsert into Delta table

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, f"{GLUE_DATABASE}.{TARGET_TABLE}_delta")

updates = [
    (2, "Bob", "Widget B", 10, 42.00),   # updated quantity & price
    (8, "Hank", "Widget D", 4, 99.99),   # new row
]

df_updates = spark.createDataFrame(updates, schema).withColumn(
    "order_date", F.current_timestamp()
)

(
    delta_table.alias("target")
    .merge(df_updates.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Merge complete.")
display(spark.table(f"{GLUE_DATABASE}.{TARGET_TABLE}_delta").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inspect Table Metadata in Glue

# COMMAND ----------

# Show all tables in the Glue database
display(spark.sql(f"SHOW TABLES IN {GLUE_DATABASE}"))

# COMMAND ----------

# Describe a table's schema and properties
spark.sql(
    f"DESCRIBE EXTENDED {GLUE_DATABASE}.{TARGET_TABLE}_delta"
).show(100, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cleanup (Optional)

# COMMAND ----------

# Uncomment to drop the tables and database
# spark.sql(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{TARGET_TABLE}_managed")
# spark.sql(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{TARGET_TABLE}_external")
# spark.sql(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{TARGET_TABLE}_delta")
# spark.sql(f"DROP DATABASE IF EXISTS {GLUE_DATABASE}")
# print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes & Limitations (Legacy Glue Integration)
# MAGIC
# MAGIC | Topic | Detail |
# MAGIC |---|---|
# MAGIC | **Metastore config** | Set `spark.hadoop.hive.metastore.client.factory.class` to `com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory` in the cluster Spark config |
# MAGIC | **IAM permissions** | The cluster's instance profile / IAM role needs `glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`, `glue:DeleteTable`, `glue:GetPartitions`, etc. |
# MAGIC | **Two-level namespace** | Legacy Glue uses `database.table` (no catalog prefix). Unity Catalog uses `catalog.schema.table`. |
# MAGIC | **Delta support** | Delta tables are registered in Glue as `EXTERNAL_TABLE` with a `delta` SerDe. Other engines (Athena, Redshift Spectrum) can query them via the Delta Uniform / symlink manifest. |
# MAGIC | **Partition management** | For partitioned non-Delta tables you must run `MSCK REPAIR TABLE` or `ALTER TABLE ADD PARTITION` to sync partitions to Glue. Delta handles this automatically. |
# MAGIC | **Migration path** | Databricks recommends migrating to **Unity Catalog** with an external Glue connection for new workloads. |
