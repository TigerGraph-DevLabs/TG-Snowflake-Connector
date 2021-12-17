package com.tigergraph.spark_connector.reader

import java.io.{DataInputStream, File, FileInputStream}
import java.util.concurrent.ConcurrentHashMap
import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


class SnowFlakeReader(val readerName: String, val path: String, val password: String = "") extends Reader with Cloneable with Logging with Serializable {

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
  private val PARAM_SF_ROLE= "sfrole"

  def this(path: String, password: String) = {
    // default parameters
    this("SfReader", path, password)
    init(path)
    initSfConf(password)
  }

  private def initSfConf(password: String): Unit = {
    sfConf.put(SF_URL, config.get(SF_URL).toString)
    sfConf.put(SF_USER, config.get(SF_USER).toString)

    if (null != password && (! password.isEmpty)) {
      sfConf.put(SF_PASSWORD, password)
    } else if (null != config.get(SF_PASSWORD)) {
      sfConf.put(SF_PASSWORD, config.get(SF_PASSWORD).toString)
    }

    sfConf.put(SF_DATABASE, config.get(SF_DATABASE).toString)
    sfConf.put(SF_SCHEMA, config.get(SF_SCHEMA).toString)

    if (null != config.get(SF_WAREHOUSE)) {
     sfConf.put(SF_WAREHOUSE, config.get(SF_WAREHOUSE).toString)
    }

    if (null != config.get(SF_APPLICATION)) {
      sfConf.put(SF_APPLICATION, config.get(SF_APPLICATION).toString)
    }

    if (null != config.get(PARAM_SF_ROLE)) {
      sfConf.put(PARAM_SF_ROLE, config.get(PARAM_SF_ROLE).toString)
    }

    if (! StringUtils.isEmpty(getPemPrivateKey())) {
      sfConf.put(PARAM_PEM_PRIVATE_KEY, getPemPrivateKey())
    }

    val tableList: util.List[String] = config.get(SF_DBTABLE).asInstanceOf[util.List[String]]
    if (tableList != null) {
      for (elem <- tableList) {
        tables += elem
      }
    }
  }

  private def getPemPrivateKey(): String = {
    if (null ==  config.get(PARAM_PEM_PRIVATE_KEY)) {
      return ""
    }

    val pemPath: String = config.get(PARAM_PEM_PRIVATE_KEY).toString

    val f = new File(pemPath)
    val fis = new FileInputStream(f)
    val dis = new DataInputStream(fis)
    val keyBytes = new Array[Byte](f.length.asInstanceOf[Int])
    dis.readFully(keyBytes)
    dis.close()

    var encrypted = new String(keyBytes, "utf-8")
    encrypted = encrypted.replaceAll("-*BEGIN .* KEY-*", "")
    encrypted = encrypted.replaceAll("-*END .* KEY-*", "")
    encrypted.trim
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
