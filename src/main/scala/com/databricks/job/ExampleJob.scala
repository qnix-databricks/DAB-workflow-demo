package com.databricks.job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Example Databricks job that can be run as a JAR job.
 */
object ExampleJob {
  
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
    runDate: String = "",
    outputEnvironment: String = "",
    overwrite: String = "",
    appConfig: String = ""
  )
  
  def parseArgs(args: Array[String]): JobConfig = {
    // Simple argument parsing
    // Expected format: --runDate <date>
    //                  --outputEnvironment <env>
    //                  --overwrite <overwrite>
    //                  --appConfig <config>
    val argMap = args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => 
        key.stripPrefix("--") -> value
    }.toMap
    
    JobConfig(
      runDate = argMap.getOrElse("runDate", ""),
      outputEnvironment = argMap.getOrElse("outputEnvironment", ""),
      overwrite = argMap.getOrElse("overwrite", ""),
      appConfig = argMap.getOrElse("appConfig", "{}"),
    )
  }
}