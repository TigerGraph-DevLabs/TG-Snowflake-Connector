package com.tigergraph.spark_connector


import com.tigergraph.spark_connector.reader.{Reader, SnowFlakeReader}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.ArrayBuffer

object SF2Spark2TgDemo {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val sfReader: Reader = new SnowFlakeReader()
    val tables: ArrayBuffer[String] = sfReader.getTables
    val dfReader: DataFrameReader = sfReader.reader(spark)

    for (table <- tables) {
      val df: DataFrame = sfReader.readTable(dfReader, table)
      df.show()
    }


    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    //
    // Configure your Snowflake environment
    //
    var sfOptions = Map(
      "sfURL" -> "lla10179.us-east-1.snowflakecomputing.com",
      "sfUser" -> "liusx",
      "sfPassword" -> "WxW7b6xJtWJpUGt",
      "sfDatabase" -> "tg_spark",
      "sfSchema" -> "synthea10k"
    )

    val df: DataFrame = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", "patients")
      .load()

    df.show()

    //
    // Create a temporary Patients
    //
    df.createOrReplaceTempView("tmpPatients")
    // Rename variable & Converts the time format to a string(yyyy/MM/dd HH:mm:ss)
    val renameDF = spark.sql("SELECT `\"Id\"` as v_id, date_format( BIRTHDATE,'yyyy/MM/dd HH:mm:ss') as DateOfBirth,  date_format(DEATHDATE,'yyyy/MM/dd HH:mm:ss') as DateOfDeath," +
      "SSN,DRIVERS as DL,PASSPORT as Passport,PREFIX as Prefix,FIRST as FirstName ,LAST as LastName,SUFFIX as Suffix,MAIDEN as MaidenName,MARITAL as MaritalStatus" +
      ",RACE as Race,ETHNICITY as Ethnicity,GENDER as Gender FROM tmpPatients")

    // formatted null value
    val formattedDF = renameDF.na.fill("1900/01/01 00:00:00", Array("DateOfDeath")).na.fill("")

    formattedDF.write.mode("overwrite").format("jdbc").options(
      Map(
        "driver" -> "com.tigergraph.jdbc.Driver",
        "url" -> "jdbc:tg:http://18.163.35.43:14240",
        "username" -> "tigergraph",
        "password" -> "ces123",
        "graph" -> "synthea", // graph name
        "dbtable" -> "vertex Patient", // loading job name
        "batchsize" -> "200",
        "debug" -> "0"))
      .save()

    spark.close()

  }

}
