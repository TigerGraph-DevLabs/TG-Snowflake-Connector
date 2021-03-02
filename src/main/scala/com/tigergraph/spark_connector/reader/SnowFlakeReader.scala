package com.tigergraph.spark_connector.reader

import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.ArrayBuffer

class SnowFlakeReader(val readerName: String) extends Reader with Cloneable with Logging with Serializable {

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
  private var TBSTRING = ""

  def this() = {
    this("SfReader")
    init()
    initSfConf()
  }

  private def initSfConf(): Unit = {
    sfConf.put(SF_URL, properties.getProperty(SF_URL))
    sfConf.put(SF_USER, properties.getProperty(SF_USER))
    sfConf.put(SF_PASSWORD, properties.getProperty(SF_PASSWORD))
    sfConf.put(SF_DATABASE, properties.getProperty(SF_DATABASE))
    sfConf.put(SF_SCHEMA, properties.getProperty(SF_SCHEMA))
    sfConf.put(SF_WAREHOUSE, properties.getProperty(SF_WAREHOUSE))
    TBSTRING = properties.getProperty(SF_DBTABLE)
  }

  override def reader(spark: SparkSession): DataFrameReader = {
    checkSfConf
    val reader: DataFrameReader = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfConf)
    reader
  }

  override def getTables(): ArrayBuffer[String] = {
    if (StringUtils.isNotEmpty(TBSTRING) && tables.size < 1) {
      tables ++= TBSTRING.split(",").map(_.trim)
    } else {
      throw new NullPointerException("null snow flake table")
    }
    tables
  }

  override def readTable(df: DataFrameReader, table: String): DataFrame = {
    df.option(DBTABLE, table)
      .load()
  }

  /** Set a configuration variable. */
  private def set(key: String, value: String): SnowFlakeReader = {
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
