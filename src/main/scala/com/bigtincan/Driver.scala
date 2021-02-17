package com.bigtincan

import com.bigtincan.utils.SystemParams
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, split, when}
import org.apache.spark.sql.types.DateType

object Driver {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("bigtincan")
      .master("local[*]")
      .getOrCreate()

    val inputDF = spark.
      read.
      option("header", true).
      option("inferSchema", true).
      json(SystemParams.getInputPath.get + "/test_json/")

    import spark.implicits._
    val valueSchema = spark
      .read
      .json(inputDF.select("value").as[String])
      .schema

    val intermediateDF = inputDF.
      select(
        col("*"),
        from_json($"value", valueSchema).as("valueFlattened")
      ).select(
      when(split(col("__topic"), "\\.")(2).equalTo("btc_player_data"),
        col("valueFlattened.after")) as "btc_player_data",
      when(split(col("__topic"), "\\.")(2).equalTo("relationships_groups"),
        col("valueFlattened.after")) as "relationships_groups",
      when(split(col("__topic"), "\\.")(2).equalTo("hubshare_forward"),
        col("valueFlattened.after")) as "hubshare_forward",
      split(col("__topic"), "\\.")(2) as "__table",
      col("__timestamp").cast(DateType) as "date",
      col("__key"),
      col("__topic"),
      col("__partition"),
      col("__offset"),
      col("valueFlattened.op") as "__action",
      col("__timestamp")
    )
      .drop("valueFlattened")

    intermediateDF
      .write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .parquet(SystemParams.getOutputPath.get)
  }

}
