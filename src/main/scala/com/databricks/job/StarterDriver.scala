package com.databricks.job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Example Databricks job that can be run as a JAR job.
 * 
 * This job demonstrates basic Spark operations:
 * - Reading data
 * - Transforming data
 * - Writing results
 */
object StarterDriver {
  
  def main(args: Array[String]): Unit = {
    // Create Spark session
    // Note: In Databricks, appName is managed by the cluster and should not be set
    val spark = SparkSession.builder().getOrCreate()

    // Parse command line arguments
    val config = parseArgs(args)
    println(s"config: $config")
    
    for (arg <- args) {
      println(arg)
    }
  }
  
  case class JobConfig(
    configPath: String = "",
    selectedEnv: String = "",
    runIdString: String = "",
    taskName: String = "",
    notes: String = "",
    inputPath: String = "",
    outputPath: String = ""
  )
  
  def parseArgs(args: Array[String]): JobConfig = {
    // Simple argument parsing
    // Expected format: --configPath <path>
    //                  --env <path>
    //                  --parent_run_id <parent_run_id>
    //                  --task_name <task_name>
    //                  --notes <notes>
    //                  --inputPath <inputPath>
    //                  --outputPath <inputPath>
    val argMap = args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => 
        key.stripPrefix("--") -> value
    }.toMap
    
    JobConfig(
      configPath = argMap.getOrElse("configPath", ""),
      selectedEnv = argMap.getOrElse("env", ""),
      runIdString = argMap.getOrElse("parent_run_id", ""),
      taskName = argMap.getOrElse("task_name", ""),
      notes = argMap.getOrElse("notes", ""),
      inputPath = argMap.getOrElse("inputPath", ""),
      outputPath = argMap.getOrElse("outputPath", "")
    )
  }
}

