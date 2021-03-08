package com.tigergraph.spark_connector.reader

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.ArrayBuffer

trait Reader {
  val properties = new Properties()
  val DBTABLE = "dbtable"

  def init(path:String) = {
    properties.load(new FileInputStream(path))
  }

  def reader(spark: SparkSession): DataFrameReader

  def getTables(): ArrayBuffer[String]

  def readTable(df: DataFrameReader, table: String): DataFrame

}


