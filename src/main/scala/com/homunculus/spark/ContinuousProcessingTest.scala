package com.homunculus.spark

import org.apache.spark.sql.SparkSession


object ContinuousProcessingTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ContinuousProcessingTest")
      .master("local[*]")
      .getOrCreate()


    //    val kafkaSourceDF = spark
    //      .readStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", "192.168.11.30:9092,192.168.11.33:9092,192.168.11.35:9092")
    //      .option("subscribe", "redcliff.order")
    //      .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //
    //    kafkaSourceDF.writeStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", "192.168.11.30:9092,192.168.11.33:9092,192.168.11.35:9092")
    //      .option("topic", "spark_continuous_processing_test")
    //      .option("checkpointLocation", "checkpoint")
    //      .trigger(Trigger.Continuous("1 second")) // only change in query
    //      .start()


    spark
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
      //.trigger(Trigger.Continuous(100))  // only change in query
      .start()


  }
}



