# Databricks notebook source
# MAGIC %md
# MAGIC # Compute Runtime Parameters
# MAGIC
# MAGIC Replaces bash-based dynamic parameter evaluation from EMR/Airflow.
# MAGIC Computes dates, lookback windows, and environment settings, then passes
# MAGIC them downstream via task values.
# MAGIC
# MAGIC When `run_date` is provided (e.g., during a backfill), it is used as-is.
# MAGIC Otherwise, defaults to yesterday.

# COMMAND ----------

from datetime import datetime, timedelta

dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD), blank = yesterday")
dbutils.widgets.text("lookback_days", "7", "Lookback window in days")
dbutils.widgets.text("env", "dev", "Environment (dev/staging/prod)")

# COMMAND ----------

run_date_input = dbutils.widgets.get("run_date").strip()
lookback_days = int(dbutils.widgets.get("lookback_days"))
env = dbutils.widgets.get("env").strip()

if run_date_input:
    run_date = datetime.strptime(run_date_input, "%Y-%m-%d")
else:
    run_date = datetime.now() - timedelta(days=1)

start_date = run_date - timedelta(days=lookback_days)

print(f"run_date:    {run_date.strftime('%Y-%m-%d')}")
print(f"start_date:  {start_date.strftime('%Y-%m-%d')}")
print(f"lookback:    {lookback_days} days")
print(f"environment: {env}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="run_date", value=run_date.strftime("%Y-%m-%d"))
dbutils.jobs.taskValues.set(key="start_date", value=start_date.strftime("%Y-%m-%d"))
dbutils.jobs.taskValues.set(key="end_date", value=run_date.strftime("%Y-%m-%d"))
dbutils.jobs.taskValues.set(key="env", value=env)
