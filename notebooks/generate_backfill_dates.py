# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Backfill Date List
# MAGIC
# MAGIC This notebook generates a JSON array of dates for use with `for_each_task`.
# MAGIC It accepts a start and end date, and outputs the date list as a task value
# MAGIC that downstream tasks can iterate over.

# COMMAND ----------

from datetime import datetime, timedelta
import json

dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")
dbutils.widgets.text("step_days", "1", "Step size in days")

# COMMAND ----------

start_str = dbutils.widgets.get("start_date")
end_str = dbutils.widgets.get("end_date")
step_days = int(dbutils.widgets.get("step_days"))

if not start_str or not end_str:
    raise ValueError("Both start_date and end_date are required parameters.")

start = datetime.strptime(start_str, "%Y-%m-%d")
end = datetime.strptime(end_str, "%Y-%m-%d")

if start > end:
    raise ValueError(f"start_date ({start_str}) must be before end_date ({end_str})")

dates = []
current = start
while current <= end:
    dates.append(current.strftime("%Y-%m-%d"))
    current += timedelta(days=step_days)

print(f"Generated {len(dates)} dates from {start_str} to {end_str} (step={step_days} day(s))")
print(f"First: {dates[0]}, Last: {dates[-1]}")

# COMMAND ----------

# for_each_task expects a JSON array string
dbutils.jobs.taskValues.set(key="date_list", value=json.dumps(dates))
