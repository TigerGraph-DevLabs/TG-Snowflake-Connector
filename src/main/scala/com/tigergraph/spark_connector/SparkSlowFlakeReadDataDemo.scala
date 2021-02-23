package com.tigergraph.spark_connector

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSlowFlakeReadDataDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    //
    // Configure your Snowflake environment
    //
    var sfOptions = Map(
      "sfURL" -> "lla10179.us-east-1.snowflakecomputing.com",
      "sfUser" -> "liusx",
      "sfPassword" -> "WxW7b6xJtWJpUGt",
      "sfDatabase" -> "tg_spark",
      "sfSchema" -> "synthea10k"
      //    "sfWarehouse" -> "<warehouse>"
    )

    val df: DataFrame = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", "careplans")
      .load()

    df.show()

    spark.close()

  }
}
