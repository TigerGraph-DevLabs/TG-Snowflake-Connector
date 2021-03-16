package com.tigergraph.spark_connector.writer

import java.io.FileInputStream
import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor


class TigerGraphWriter(loadDefaults: Boolean, configPath: String) extends Cloneable with Logging with Serializable {
  private val tgConf = new ConcurrentHashMap[String, String]()

  var config: util.HashMap[String, Object] = _

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
  private val NUM_PARTITIONS = "numPartitions"

  def this(configPath: String) = {
    this(true, configPath)
    init()
    initSfConf()
  }

  def init() = {
    val yaml = new Yaml(new Constructor(classOf[util.HashMap[String, Object]]))
    config = yaml.load(new FileInputStream(configPath)).asInstanceOf[util.HashMap[String, Object]]
  }

  private def initSfConf(): Unit = {
    tgConf.put(DRIVER, config.get(DRIVER).toString)
    tgConf.put(URL, config.get(URL).toString)
    tgConf.put(USERNAME, config.get(USERNAME).toString)
    tgConf.put(PASSWORD, config.get(PASSWORD).toString)
    tgConf.put(GRAPH, config.get(GRAPH).toString)
    tgConf.put(FILE_NAME, config.getOrDefault(FILE_NAME, "").toString)
    tgConf.put(SEP, config.getOrDefault(SEP, "").toString)
    tgConf.put(EOL, config.getOrDefault(EOL, "\n").toString)
    tgConf.put(EOL, "\n")
    tgConf.put(BATCHSIZE, config.getOrDefault(BATCHSIZE, "").toString)
    tgConf.put(DEBUG, config.getOrDefault(DEBUG, "").toString)
    tgConf.put(NUM_PARTITIONS, config.getOrDefault(NUM_PARTITIONS, "1").toString)
  }

  def insertData(data: Iterator[(Int, Row)]): Unit = {

  }

  def write(df: DataFrame, dbtable: String, schema: String): Unit = {
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
