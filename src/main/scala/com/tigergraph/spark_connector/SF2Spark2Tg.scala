package com.tigergraph.spark_connector


import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Future => _, _}
import java.util.regex.Pattern

import com.tigergraph.spark_connector.reader.{Reader, SnowFlakeReader}
import com.tigergraph.spark_connector.support.{SnowFlakeSupport, Support}
import com.tigergraph.spark_connector.utils.ProgressUtil
import com.tigergraph.spark_connector.writer.TigerGraphWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._

object SF2Spark2Tg {

  // default path
  var CONFIG_PATH: String = _
  val totalNum: AtomicLong = new AtomicLong(1)
  val successNum: AtomicLong = new AtomicLong(0)
  var statusChange: Boolean = false

  val loadSuccessLogger = Logger.getLogger("loadSuccess")
  val loadBeginLogger = Logger.getLogger("loadBegin")
  val progressLogger = Logger.getLogger("progress")

  Logger.getLogger("loadSuccess").setLevel(Level.INFO)
  Logger.getLogger("loadBegin").setLevel(Level.INFO)
  Logger.getLogger("progress").setLevel(Level.INFO)

  private val TIME_OUT = "timeout"
  private val TIME_UNIT = "timeunit"


  def dfFilteredAndFormatted(oldDF: DataFrame, filteredStr: String): DataFrame = {

    var snowFlakeColumns = ArrayBuffer[String]()

    filteredStr.split(",").foreach(column => {
      snowFlakeColumns += column
    })


    val filteredDF = oldDF.select(snowFlakeColumns.map(oldDF.col(_)): _*)
    var dfMiddle = filteredDF

    // select date type
    val dateArray = dfMiddle.schema.filter(schema => {
      schema.dataType.typeName == "date" || schema.dataType.typeName == "timestamp"
    }).map(_.name).toArray

    // find decimal(*,0) format  convert to long
    val pattern = "(decimal)\\((\\d)*,0\\)"

    // select decimal type
    val intTypeArray = dfMiddle.schema.filter(schema => {
      // int bigint 转换成 long
      val name = schema.dataType.typeName
      // schema.dataType.typeName.contains("decimal(38,0)")
      Pattern.matches(pattern, name)
    }).map(_.name).toArray

    val decimalTypeArray = dfMiddle.schema.filter(schema => {
      // int bigint 转换成 long
      schema.dataType.typeName.contains("decimal")
    }).map(_.name).filter(name => {
      !intTypeArray.contains(name)
    }).toArray

    for (colName <- dateArray) {
      // convert date to string
      dfMiddle = dfMiddle.withColumn(colName, date_format(col(colName), "yyyy/MM/dd HH:mm:ss"))
    }

    for (colName <- intTypeArray) {
      // convert decimal to int
      dfMiddle = dfMiddle.withColumn(colName, col(colName).cast("long"))
    }

    for (colName <- decimalTypeArray) {
      // convert decimal to int
      dfMiddle = dfMiddle.withColumn(colName, col(colName).cast(DoubleType))
    }

    // fill null date
    dfMiddle = dfMiddle.na.fill("1900/01/01 00:00:00", dateArray).na.fill("")

    dfMiddle
  }

  def writeDF2Tiger(spark: SparkSession, df: DataFrame, table: String, support: Support)(implicit xc: ExecutionContext) = Future {

    // get jobName and columnStr
    val jobNameAndColumn = support.getTableInfo(table)

    val jobName = jobNameAndColumn._1
    val columnStr = jobNameAndColumn._2

    val loadingInfo = support.getLoadingJobInfo(table)

    val startTime = System.currentTimeMillis()

    loadBeginLogger.info(s"[ ${table} ] begin load data")

    val dfFormatted = dfFilteredAndFormatted(df, columnStr)

    val tgWriter: TigerGraphWriter = new TigerGraphWriter(CONFIG_PATH)

    try {
      tgWriter.write(dfFormatted, jobName, columnStr, loadingInfo)
    } catch {
      case e: Exception =>
        // check tg conf illegal
        // not token or username and password
        e.printStackTrace()
        System.exit(-1)
    }

    successNum.getAndIncrement()
    statusChange = true
    loadSuccessLogger.info(s"[ ${table} ] load success, consume time: ${(System.currentTimeMillis() - startTime) / 1000} s")

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

//    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).getOrCreate()

    val spark = SparkSession.builder().master("local").appName(this.getClass.getCanonicalName).getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    // Set number of threads via a configuration property
    val pool = Executors.newFixedThreadPool(30)
    val singlePool = Executors.newSingleThreadExecutor()

    // create the implicit ExecutionContext based on our thread pool
    implicit val xc = ExecutionContext.fromExecutorService(pool)

    val sfReader: Reader = new SnowFlakeReader(path)
    val snowFlakeSupport: Support = new SnowFlakeSupport(path)
    val tables: ArrayBuffer[String] = sfReader.getTables
    val dfReader: DataFrameReader = sfReader.reader(spark)

    val tgWriter = new TigerGraphWriter(path)

    val config = tgWriter.config
    val timeOut: Integer = config.getOrDefault(TIME_OUT, new Integer(24)).asInstanceOf[Integer]
    val timeUnit = config.getOrDefault(TIME_UNIT, "HOURS").asInstanceOf[String]

    val tasks: ArrayBuffer[Future[Unit]] = new ArrayBuffer[Future[Unit]]()

    for (table <- tables) {
      val df: DataFrame = sfReader.readTable(dfReader, table)
      val task = writeDF2Tiger(spark, df, table, snowFlakeSupport)
      tasks += task
    }

    totalNum.set(tables.length)

    // add logging task
    addLogDaemon(singlePool)

    try {
      // await task complete
      Await.result(Future.sequence(tasks), Duration(timeOut.longValue(), TimeUnit.valueOf(timeUnit)))
    } catch {
      case e: Exception =>
        progressLogger.error("Wait timeout, try batch import")
        e.printStackTrace()
    }

    pool.shutdownNow()
    singlePool.shutdownNow()
    spark.close()
    progressLogger.info("The total time consuming:" + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }

  def addLogDaemon(singlePool: ExecutorService): Unit = {
    val future = singlePool.submit(new Callable[Unit]() {
      override def call(): Unit = {
        var flag = true

        var remainTime = 5000
        var lastLogTime = System.currentTimeMillis()

        while (flag) {
          // 1 second check once
          Thread.sleep(1000)
          // If there is a successful write or more than 5S after the last print
          if (statusChange || (System.currentTimeMillis() - lastLogTime) > 5000) {
            val success = successNum.get()
            val total = totalNum.get()

            progressLogger.info(ProgressUtil.drawLine((success * 100 / total).toInt))

            if (success == total) {
              flag = false
            }
            // put status to false, represent one write success flag
            statusChange = false
            lastLogTime = System.currentTimeMillis()
          }
        }

      }
    })
  }

}
