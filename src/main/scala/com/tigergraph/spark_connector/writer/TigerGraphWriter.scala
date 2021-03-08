package com.tigergraph.spark_connector.writer

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame


class TigerGraphWriter(loadDefaults: Boolean, configPath: String) extends Cloneable with Logging with Serializable {
  private val tgConf = new ConcurrentHashMap[String, String]()
  val properties = new Properties()

  private val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
  private val DRIVER = "driver"
  private val URL = "url"
  private val USERNAME = "username"
  private val PASSWORD = "password"
  private val GRAPH = "graph"
  private val FILE_NAME = "filename"
  private val SEP = "sep"
  private val EOL = "eol"
  private val BATCHSIZE = "batchsize"
  private val DEBUG = "debug"

  //  def this() = {
  //    this(true, "/connector/connector.properties")
  //    init()
  //    initSfConf()
  //  }

  def this(configPath: String) = {
    this(true, configPath)
    init()
    initSfConf()
  }

  def init() = {
    properties.load(new FileInputStream(configPath))
  }

  private def initSfConf(): Unit = {
    tgConf.put(DRIVER, properties.getProperty(DRIVER))
    tgConf.put(URL, properties.getProperty(URL))
    tgConf.put(USERNAME, properties.getProperty(USERNAME))
    tgConf.put(PASSWORD, properties.getProperty(PASSWORD))
    tgConf.put(GRAPH, properties.getProperty(GRAPH))
    tgConf.put(FILE_NAME, properties.getProperty(FILE_NAME))
    tgConf.put(SEP, properties.getProperty(SEP))
    tgConf.put(EOL, properties.getProperty(EOL))
    tgConf.put(BATCHSIZE, properties.getProperty(BATCHSIZE))
    tgConf.put(DEBUG, properties.getProperty(DEBUG))
  }

  def
  write(df: DataFrame, dbtable: String, schema: String): Unit = {
    checkTgConf

    df.write.mode("overwrite").format("jdbc")
      .options(tgConf)
      .options(
        Map(
          "dbtable" -> dbtable, // loading job name
          "schema" -> schema // column definitions
        )).save()

  }

  /** Set a configuration variable. */
  private def set(key: String, value: String): TigerGraphWriter = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }

    tgConf.put(key, value)
    this
  }

  def checkTgConf(): Unit = {
    if (StringUtils.isEmpty(tgConf.get(DRIVER))) {
      throw new NullPointerException("null tiger graph driver")
    }
    if (StringUtils.isEmpty(tgConf.get(URL))) {
      throw new NullPointerException("null tiger graph url")
    }
    if (StringUtils.isEmpty(tgConf.get(USERNAME))) {
      throw new NullPointerException("null tiger graph username")
    }
    if (StringUtils.isEmpty(tgConf.get(PASSWORD))) {
      throw new NullPointerException("null tiger graph password")
    }
    if (StringUtils.isEmpty(tgConf.get(GRAPH))) {
      throw new NullPointerException("null tiger graph graph")
    }
  }
}
