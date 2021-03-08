package com.tigergraph.spark_connector


import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.Executors

import com.tigergraph.spark_connector.reader.{Reader, SnowFlakeReader}
import com.tigergraph.spark_connector.utils.MapUtil
import com.tigergraph.spark_connector.writer.TigerGraphWriter
import net.minidev.json.JSONObject
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._

object SF2Spark2Tg {

  // default path
  var CONFIG_PATH: String = "/connector/connector.properties"

  def dfFilteredAndFormatted(oldDF: DataFrame, filteredStr: String, formattedStr: String): DataFrame = {

    var snowFlakeColumns = ArrayBuffer[String]()
    var tigerColumns = ArrayBuffer[String]()

    filteredStr.split(",").foreach(column => {
      snowFlakeColumns += column
    })

    formattedStr.split(",").foreach(column => {
      tigerColumns += column
    })

    val filteredDF = oldDF.select(snowFlakeColumns.map(oldDF.col(_)): _*)
    var dfMiddle = oldDF

    val columnsRenamed = tigerColumns.toIndexedSeq
    // rename snowFlake's  dataFrame column name to tigerGraph column name
    dfMiddle = filteredDF.toDF(columnsRenamed: _*)

    // select date type
    val dateArray = dfMiddle.schema.filter(schema => {
      schema.dataType.typeName == "date" || schema.dataType.typeName == "timestamp"
    }).map(_.name).toArray

    // select decimal type
    val bigDecimalArray = dfMiddle.schema.filter(schema => {
      schema.dataType.typeName.contains("decimal")
    }).map(_.name).toArray

    for (colName <- dateArray) {
      // convert date to string
      dfMiddle = dfMiddle.withColumn(colName, date_format(col(colName), "yyyy/MM/dd HH:mm:ss"))
    }

    for (colName <- bigDecimalArray) {
      // convert decimal to int
      dfMiddle = dfMiddle.withColumn(colName, col(colName).cast("int"))
    }

    // fill null date
    dfMiddle = dfMiddle.na.fill("1900/01/01 00:00:00", dateArray).na.fill("")

    dfMiddle
  }


  def writeDF2Tiger(spark: SparkSession, df: DataFrame, table: String)(implicit xc: ExecutionContext) = Future {

    val properties = new Properties()

    properties.load(new FileInputStream(CONFIG_PATH))

    println("-" * 50)
    println(s"${table} begin load data")

    // snowFlake column name -> tigerGraph column name
    val sf2TigerKV = MapUtil.convertString2Map(properties.get(table).toString)

    val jobName = sf2TigerKV.get("dbtable").toString

    val tigerMap = sf2TigerKV.get("jobConfig").asInstanceOf[JSONObject]
    val sfColumnStr = tigerMap.get("sfColumn").toString
    val tigerColumnStr = tigerMap.get("tigerColumn").toString
    val dfFormatted = dfFilteredAndFormatted(df, sfColumnStr, tigerColumnStr)

    val tgWriter: TigerGraphWriter = new TigerGraphWriter(CONFIG_PATH)

    tgWriter.write(dfFormatted, jobName, tigerColumnStr)


    println(s"${table} load success......")
    println("=" * 50)

  }

  def main(args: Array[String]): Unit = {

    if (args == null || args.length == 0) {
      println("no config path")
      return
    }

    val startTime = System.currentTimeMillis()

    val path = args(0)
    println(s"config path is ${path} ")

    CONFIG_PATH = path

    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    // Set number of threads via a configuration property
    val pool = Executors.newFixedThreadPool(10)

    // create the implicit ExecutionContext based on our thread pool
    implicit val xc = ExecutionContext.fromExecutorService(pool)

    val sfReader: Reader = new SnowFlakeReader(path)
    val tables: ArrayBuffer[String] = sfReader.getTables
    val dfReader: DataFrameReader = sfReader.reader(spark)

    val tasks: ArrayBuffer[Future[Unit]] = new ArrayBuffer[Future[Unit]]()

    for (table <- tables) {
      val df: DataFrame = sfReader.readTable(dfReader, table)
      val task = writeDF2Tiger(spark, df, table)
      tasks += task
    }

    // await task complete
    Await.result(Future.sequence(tasks), Duration(1, HOURS))

    spark.close()
    println("The total time consuming:" + (System.currentTimeMillis() - startTime) / 1000 + "s")
    pool.shutdown()

  }

}
