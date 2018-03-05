package com.homunculus.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object ContinuousProcessingTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ContinuousProcessingTest")
      .master("local[*]")
      .getOrCreate()


    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "spark_test_from")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "spark_test_to")
      .option("checkpointLocation", "checkpoint")
      .trigger(Trigger.Continuous(100))  // only change in query
      .start()
    query.awaitTermination()
  }
}



