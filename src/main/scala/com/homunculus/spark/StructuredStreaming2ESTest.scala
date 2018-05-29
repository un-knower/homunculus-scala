package com.homunculus.spark

import java.text.SimpleDateFormat

import com.beust.jcommander.JCommander
import com.google.gson.{Gson, GsonBuilder}
import com.homunculus.common.{CanalModel, CommandArgs}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * StructuredStreaming2ESTest
 *
 * @author Jade
 */
object StructuredStreaming2ESTest {
  def main(args: Array[String]): Unit = {

    classOf[JCommander]
      .getConstructor(classOf[Object])
      .newInstance(CommandArgs)
      .parse(args: _*)
    println(CommandArgs.toString)

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
      .option("kafka.bootstrap.servers", CommandArgs.brokers)
      .option("group.id", CommandArgs.groupId)
      .option("subscribe", CommandArgs.topic)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .map(($: Row) => {
        val gson: Gson = new GsonBuilder().serializeNulls.create
        val canalModel = gson.fromJson($.getString(1), classOf[CanalModel])
        val dmlType = canalModel.dmlType.toLowerCase
        val column = if (dmlType.equals("delete")) canalModel.beforeColumn else canalModel.afterColumn
        val esModel = gson.fromJson(gson.toJson(column), classOf[OrderAbandonLogType]).init
        (dmlType, esModel)
      })
      .filter($ => !(java.util.Objects.equals($._1, "delete")))
      .map($ => $._2)


    println(s"query.schema = ${query.schema}")


    query.writeStream
      .outputMode(OutputMode.Append)
      .format("org.elasticsearch.spark.sql")
      .trigger(Trigger.ProcessingTime(CommandArgs.interval))
      .option("checkpointLocation", "checkpoint")
      .option("es.mapping.id", "id")
      .option("es.nodes", CommandArgs.esNodes)
      .option("es.port", CommandArgs.esPort)
      .option("es.index.auto.create", "true")
      .option("es.write.operation", "upsert")
      .option("es.update.retry.on.conflict", "10")
      .option("es.spark.dataframe.write.null", "true")
      .option("es.mapping.exclude", "index_pt")
      .option("es.resource.write", "ss_order_abandon_log-{index_pt}/ss_order_abandon_log")
      .queryName("ElasticSink")
      .start
      .awaitTermination()

  }
}


/**
 *
 * 日期类型用var 用于空串(空串写入es日期会报错)转null (kafka过来就是空串)
 *
 * @param index_pt 动态索引指定字段
 */
case class OrderAbandonLogType(val id: java.lang.Long,
                               val city_id: java.lang.Integer,
                               val order_id: java.lang.Long,
                               val rider_id: java.lang.Integer,
                               val reason: java.lang.Integer,
                               val why: java.lang.String,
                               val order_status: java.lang.Byte,
                               var switch_tm: java.lang.String,
                               var dispatch_tm: java.lang.String,
                               var arrive_tm: java.lang.String,
                               val dispatch_mode: java.lang.Byte,
                               val source: java.lang.Byte,
                               val shardx: java.lang.Integer,
                               val shardz: java.lang.Integer,
                               val feature: java.lang.String,
                               var index_pt: java.lang.String) {
  def init(): OrderAbandonLogType = {
    if (java.util.Objects.equals(dispatch_tm, "")) {
      this.dispatch_tm = null
    }
    if (java.util.Objects.equals(arrive_tm, "")) {
      this.arrive_tm = null
    }
    val sdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ptFormat = new SimpleDateFormat("yyyyMM")
    this.index_pt = ptFormat.format(sdFormat.parse(this.switch_tm))
    this
  }
}





