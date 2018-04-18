package com.homunculus.spark

import java.util

import com.google.gson.Gson
import com.homunculus.common.{CanalModel, OrderAbandonLogType}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * StructuredStreaming2ESTest
 *
 * @author Jade
 */
object StructuredStreaming2ESTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.11.30:19092")
      .option("group.id", "StructuredStreaming2ESTest")
      .option("subscribe", "redcliff.order_abandon_log")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .map(($: Row) => {
        print($)
        val canalModel = (new Gson).fromJson($.getString(1), classOf[CanalModel])
        val dataMap = processFieldsTransToES(canalModel)
        val id: java.lang.Long = dataMap.get("id").get.asInstanceOf[Long]
        val city_id: java.lang.Long = if (dataMap.get("city_id") != None) dataMap.get("city_id").get.asInstanceOf[Long] else null

        val a = new OrderAbandonLogType(id, city_id)

      })


    println(s"query.schema = ${query.schema}")


    query.writeStream
      .outputMode(OutputMode.Append)
      .format("org.elasticsearch.spark.sql")
      .trigger(Trigger.ProcessingTime(2000))
      .option("checkpointLocation", "checkpoint")
      .option("es.mapping.id", "id")
      .option("es.nodes", "192.168.11.51")
      .option("es.port", "9200")
      .option("es.index.auto.create", "true")
      .option("es.write.operation", "upsert")
      .option("es.update.retry.on.conflict", "10")
      .option("es.spark.dataframe.write.null", "true")
      .option("es.resource.write", "bigdata-{city_id}/structured_streaming_test")
      .queryName("ElasticSink")
      .start
      .awaitTermination()

  }


  def processFieldsTransToES(canalModel: CanalModel, clazz: Class[_]): _ = {
    val dmlType = canalModel.dmlType.toLowerCase
    val column = if (dmlType.equals("delete")) canalModel.beforeColumn else canalModel.afterColumn
    val gson: Gson = new Gson
    gson.fromJson(gson.toJson(column), clazz)
  }

}
