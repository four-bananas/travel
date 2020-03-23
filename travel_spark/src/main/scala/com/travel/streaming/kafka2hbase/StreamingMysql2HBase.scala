package com.travel.streaming.kafka2hbase

import java.util.regex.Pattern

import com.travel.common.HBaseUtil
import com.travel.loggings.Logging
import com.travel.utils.{HbaseTools, JsonParse, SparkKafkaTools}
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 同步mysql to hbase
 */
object StreamingMysql2HBase extends Logging {

  def main(args: Array[String]): Unit = {

    val topics = Array("travel")
    val conf = new SparkConf()
    conf.setAppName(StreamingMysql2HBase.getClass.getName).setMaster("local[1]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sparkContext = sparkSession.sparkContext

    val streamingContext = new StreamingContext(sparkContext, Seconds(2))

    val group: String = "travel"

    val connection: Connection = HBaseUtil.getConnection

    //从Hbase获取offset
    val kafkaOffset = HbaseTools.getOffsetFromHBase(connection, topics, group)

    val kafkaParams = SparkKafkaTools.buildKafkaProperties()

    import collection.JavaConverters._
    //构建kafka source 生成kafka DStream
    val kafkaDS = SparkKafkaTools.buildSourceFromKafka(streamingContext, Pattern.compile("travel"), kafkaParams, kafkaOffset.asJava)

    kafkaDS.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val conn = HBaseUtil.getConnection
        partition.foreach(record => {
          val line = record.value()
          println(line)
          val tuple = JsonParse.parse(line)
          HbaseTools.saveBusinessDatas(tuple, conn)
        })
        conn.close()
      })
      //手动管理offset 保存到hbase
      SparkKafkaTools.saveOffsetToHBase(rdd, kafkaDS,group)
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

