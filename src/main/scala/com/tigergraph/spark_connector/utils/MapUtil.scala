package com.tigergraph.spark_connector.utils

import java.util

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

object MapUtil {
  def convertString2Map(str: String): util.HashMap[String, Object] = {

    var strWrap = str
    if(strWrap == null || strWrap == ""){
      strWrap = ""
    }

    strWrap = strWrap.toString

    val jsonParser = new JSONParser(JSONParser.MODE_JSON_SIMPLE)

    val jsonObj: JSONObject = jsonParser.parse(strWrap).asInstanceOf[JSONObject]
    return jsonObj

  }

}
