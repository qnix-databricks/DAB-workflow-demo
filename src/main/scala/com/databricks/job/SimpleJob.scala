package com.databricks.job

import org.apache.spark.sql.SparkSession
import com.databricks.sdk.scala.dbutils.DBUtils

object SimpleJob {
  
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder().getOrCreate()
    val dbutils = DBUtils.getDBUtils()
    
    import spark.implicits._

    println("====>> Job parameters:")
    for (arg <- args) {
      println(arg)
    }
  }
}

