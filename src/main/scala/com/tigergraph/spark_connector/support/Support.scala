package com.tigergraph.spark_connector.support

import java.util

/**
  * Used to parse different database configuration files
  */
trait Support {
  def getTableInfo(table: String): (String, String)

  def getLoadingJobInfo(table: String): util.HashMap[String, String]
}
