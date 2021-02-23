package com.tigergraph.spark_connector

import org.apache.spark.sql.SparkSession

object SparkTigerGraphReadDataDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    val jdbcDF1 = spark.read.format("jdbc").options(
      Map(
        "driver" -> "com.tigergraph.jdbc.Driver",
        "url" -> "jdbc:tg:http://18.163.35.43:14240",
        "username" -> "tigergraph",
        "password" -> "ces123",
        "graph" -> "synthea", // graph name
        "dbtable" -> "vertex Patient", // vertex type
        "limit" -> "10", // number of vertices to retrieve
        "debug" -> "0")).load()

    jdbcDF1.show

    spark.close()
  }
}
