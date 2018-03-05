package com.homunculus.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkSocket {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 将 lines 切分为 words
    val words = lines.as[String].flatMap(_.split(" "))

    // 生成正在运行的 word count
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
