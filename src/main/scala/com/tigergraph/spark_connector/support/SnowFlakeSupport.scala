package com.tigergraph.spark_connector.support

import java.io.FileInputStream
import java.util

import org.apache.spark.internal.Logging
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

class SnowFlakeSupport(val supportName: String, val path: String) extends Support with Cloneable with Logging with Serializable {

  var config: util.HashMap[String, Object] = _

  def init(path: String) = {
    val yaml = new Yaml(new Constructor(classOf[util.HashMap[String, Object]]))
    config = yaml.load(new FileInputStream(path)).asInstanceOf[util.HashMap[String, Object]]
  }

  def this(path: String) = {
    this("sfSupport", path)
    init(path)
  }

  def getTableInfo(table: String): (String, String) = {
    val sf2TigerKV = config.get("mappingRules")
      .asInstanceOf[util.HashMap[String, Object]].get(table).asInstanceOf[util.HashMap[String, Object]]

    val jobName = sf2TigerKV.get("dbtable").toString

    val tigerMap = sf2TigerKV.get("jobConfig").asInstanceOf[util.HashMap[String, Object]]
    val sfColumnStr = tigerMap.get("sfColumn").toString
    (jobName, sfColumnStr)
  }

  def getLoadingJobInfo(table: String): util.HashMap[String, String] = {
    val sf2TigerKV = config.get("mappingRules")
      .asInstanceOf[util.HashMap[String, Object]].get(table).asInstanceOf[util.HashMap[String, Object]]

    val tigerMap = sf2TigerKV.get("jobConfig").asInstanceOf[util.HashMap[String, Object]]
    val filename = tigerMap.get("filename").asInstanceOf[String]

    val map:util.HashMap[String, String] = new util.HashMap[String,String]()

    map.put("filename", filename)

    return map
  }

}
