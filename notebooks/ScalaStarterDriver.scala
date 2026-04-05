// Databricks notebook source
// MAGIC %md
// MAGIC # Compute runtime parameters

// COMMAND ----------

import java.time.LocalDate
import java.time.format.DateTimeFormatter

val runDate = LocalDate.now().plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
println(runDate)

// COMMAND ----------

// MAGIC %md
// MAGIC # Setup job args for downstream task

// COMMAND ----------
// ERROR: UnsupportedOperationException: jobs not currently supported
dbutils.jobs.taskValues.set("runDate", runDate)
dbutils.jobs.taskValues.set("outputEnvironment", "staging")
dbutils.jobs.taskValues.set("overwrite", true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### More complex argument pass down

// COMMAND ----------

import org.json4s._
import org.json4s.jackson.JsonMethods._

val df = spark.read.text("/Volumes/users/quan_ta/config/app_conf.json")
val content = df.collect().map(_.getString(0)).mkString
val appConfig = parse(content)

// COMMAND ----------

println(pretty(render(appConfig)))

// COMMAND ----------

implicit val formats: DefaultFormats.type = DefaultFormats
dbutils.jobs.taskValues.set("appConfig", compact(render(appConfig)))
