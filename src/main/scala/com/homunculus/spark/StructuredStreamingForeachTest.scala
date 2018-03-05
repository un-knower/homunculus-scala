package com.homunculus.spark

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

/**
 * ContinuousProcessingTest2
 * 运行通过
 * foreach 本就是持续处理 没有ContinuousTrigger操作
 *
 * @author Jade
 */
object StructuredStreamingForeachTest {
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
      .foreach(new ForeachWriter[Row] {
        def open(partitionId: Long, version: Long): Boolean = {
          // open connection
          true
        }
        def process(record: Row) = {
          print(record.getString(1))
        }
        def close(errorOrNull: Throwable): Unit = {
          // close the connection
        }
      })
      .start()

    query.awaitTermination()
  }
}
