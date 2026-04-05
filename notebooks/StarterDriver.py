# Databricks notebook source
# MAGIC %md
# MAGIC # Compute runtime parameters

# COMMAND ----------

from datetime import datetime, timedelta
runDate = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
print(runDate)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup job args for downstream task

# COMMAND ----------

dbutils.jobs.taskValues.set("runDate", runDate)
dbutils.jobs.taskValues.set("outputEnvironment", "staging")
dbutils.jobs.taskValues.set("overwrite", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### More complex argument pass down

# COMMAND ----------

import json
df = spark.read.text("/Volumes/users/quan_ta/config/app_conf.json")
content = "".join([row.value for row in df.collect()])
app_config = json.loads(content)

# COMMAND ----------

print(json.dumps(app_config, indent=4))

# COMMAND ----------

dbutils.jobs.taskValues.set("appConfig", app_config)