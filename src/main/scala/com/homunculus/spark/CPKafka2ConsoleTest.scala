package com.homunculus.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * CPKafka2ConsoleTest
 *
 * @author Jade
 */
object CPKafka2ConsoleTest {
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
      .selectExpr("CAST(value AS STRING)")

    query.writeStream
      .format("console")
      .trigger(Trigger.Continuous(100))
      .start()
      .awaitTermination()
  }
}
