package com.tigergraph.spark_connector.reader

import java.io.FileInputStream
import java.util

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.collection.mutable.ArrayBuffer

trait Reader {
  var config: util.HashMap[String, Object] = _
  val DBTABLE = "dbtable"

  def init(path: String) = {
    val yaml = new Yaml(new Constructor(classOf[util.HashMap[String, Object]]))
    config = yaml.load(new FileInputStream(path)).asInstanceOf[util.HashMap[String, Object]]
  }

  def reader(spark: SparkSession): DataFrameReader

  def getTables(): ArrayBuffer[String]

  def readTable(df: DataFrameReader, table: String): DataFrame

}


