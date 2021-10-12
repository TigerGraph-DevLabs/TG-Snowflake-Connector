package com.tigergraph.spark_connector.reader

import java.util.concurrent.ConcurrentHashMap
import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


class SnowFlakeReader(val readerName: String, val path: String) extends Reader with Cloneable with Logging with Serializable {

  private val sfConf = new ConcurrentHashMap[String, String]()
  private val tables = new ArrayBuffer[String]()

  private val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
  private val SF_URL = "sfURL"
  private val SF_USER = "sfUser"
  private val SF_PASSWORD = "sfPassword"
  private val SF_DATABASE = "sfDatabase"
  private val SF_SCHEMA = "sfSchema"
  private val SF_WAREHOUSE = "sfWarehouse"
  private val SF_DBTABLE = "sfDbtable"
  private val SF_APPLICATION = "application"
  private val PARAM_PEM_PRIVATE_KEY = "pem_private_key"

  def this(path: String) = {
    // default parameters
    this("SfReader", path)
    init(path)
    initSfConf()
  }

  private def initSfConf(): Unit = {
    sfConf.put(SF_URL, config.get(SF_URL).toString)
    sfConf.put(SF_USER, config.get(SF_USER).toString)
    sfConf.put(SF_PASSWORD, config.getOrDefault(SF_PASSWORD, "").toString)
    sfConf.put(SF_DATABASE, config.get(SF_DATABASE).toString)
    sfConf.put(SF_SCHEMA, config.get(SF_SCHEMA).toString)
    sfConf.put(SF_WAREHOUSE, config.getOrDefault(SF_WAREHOUSE, "").toString)
    sfConf.put(SF_APPLICATION, config.getOrDefault(SF_APPLICATION, "tigergraph").toString)
    sfConf.put(PARAM_PEM_PRIVATE_KEY, config.getOrDefault(PARAM_PEM_PRIVATE_KEY, "").toString)

    val tableList: util.List[String] = config.get(SF_DBTABLE).asInstanceOf[util.List[String]]
    if (tableList != null) {
      for (elem <- tableList) {
        tables += elem
      }
    }
  }

  override def reader(spark: SparkSession): DataFrameReader = {
    checkSfConf
    val reader: DataFrameReader = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfConf)
    reader
  }

  override def getTables(): ArrayBuffer[String] = {
    tables
  }

  override def readTable(df: DataFrameReader, table: String): DataFrame = {
    df.option(DBTABLE, table)
      .load()
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): SnowFlakeReader = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }

    sfConf.put(key, value)
    this
  }

  def checkSfConf(): Unit = {
    if (StringUtils.isEmpty(sfConf.get(SF_URL))) {
      throw new NullPointerException("null snow flake url")
    }
    if (StringUtils.isEmpty(sfConf.get(SF_DATABASE))) {
      throw new NullPointerException("null snow flake database")
    }
    if (StringUtils.isEmpty(sfConf.get(SF_USER))) {
      throw new NullPointerException("null snow flake user")
    }
    if (StringUtils.isEmpty(sfConf.get(SF_SCHEMA))) {
      throw new NullPointerException("null snow flake schema")
    }
  }
}
