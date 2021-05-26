package com.tigergraph.spark_connector.writer

import java.io.FileInputStream
import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor


class TigerGraphWriter(loadDefaults: Boolean, configPath: String) extends Cloneable with Logging with Serializable {
  private val tgConf = new ConcurrentHashMap[String, String]()

  var config: util.HashMap[String, Object] = _

  private val DRIVER = "driver"
  private val URL = "url"
  private val USERNAME = "username"
  private val PASSWORD = "password"
  private val TOKEN = "token"
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
    initTgConf()
  }

  def init() = {
    val yaml = new Yaml(new Constructor(classOf[util.HashMap[String, Object]]))
    config = yaml.load(new FileInputStream(configPath)).asInstanceOf[util.HashMap[String, Object]]
  }

  private def initTgConf(): Unit = {
    tgConf.put(DRIVER, config.get(DRIVER).toString)
    tgConf.put(URL, config.get(URL).toString)

    // two ways authentication
    // 1. username and password
    // 2. token
    tgConf.put(USERNAME, config.getOrDefault(USERNAME, "").toString)
    tgConf.put(PASSWORD, config.getOrDefault(PASSWORD, "").toString)
    tgConf.put(TOKEN, config.getOrDefault(TOKEN, "").toString)

    tgConf.put(GRAPH, config.get(GRAPH).toString)
    tgConf.put(BATCHSIZE, config.getOrDefault(BATCHSIZE, "").toString)
    tgConf.put(SEP, config.get(SEP).asInstanceOf[String])
    tgConf.put(EOL, config.get(EOL).asInstanceOf[String])
    tgConf.put(DEBUG, config.getOrDefault(DEBUG, "").toString)
    tgConf.put(NUM_PARTITIONS, config.getOrDefault(NUM_PARTITIONS, "150").toString)
  }


  def write(df: DataFrame, dbtable: String, schema: String, loadingInfo: util.Map[String, String]): Unit = {
    try {
      checkTgConf()
    } catch {
      case e: NullPointerException =>
        // check tg conf illegal
        // not token or username and password
        e.printStackTrace()
        System.exit(-1)
    }

    df.write.mode("overwrite").format("jdbc")
      .options(tgConf)
      .options(
        Map(
          "dbtable" -> dbtable, // loading job name
          "schema" -> schema // column definitions
        )).options(loadingInfo).save()

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
      throw new NullPointerException("there is no tiger graph driver [driver] in configuration file.")
    }
    if (StringUtils.isEmpty(tgConf.get(URL))) {
      throw new NullPointerException("there is no tiger graph url [url] in configuration file.")
    }
    checkAuthentication()

    if (StringUtils.isEmpty(tgConf.get(GRAPH))) {
      throw new NullPointerException("there is no tiger graph graph [graph] in configuration file.")
    }
  }

  def checkAuthentication(): Unit = {
    //Verify the user name and password if there is no token
    if (StringUtils.isNotEmpty(tgConf.get(TOKEN))) {
      // there is a token, the check passes
      return
    }

    if (StringUtils.isEmpty(tgConf.get(USERNAME))) {
      throw new NullPointerException("there is no tiger graph username [username] in configuration file.")
    }
    if (StringUtils.isEmpty(tgConf.get(PASSWORD))) {
      throw new NullPointerException("there is no tiger graph password [password] in configuration file.")
    }
  }
}
