package com.tigergraph.spark_connector.support

/**
  * Used to parse different database configuration files
  */
trait Support {
  def getTableInfo(table: String): (String, String);
}
