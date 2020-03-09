package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants, HBaseUtil, JedisUtil}
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object StreamingKafka {

  def main(args: Array[String]): Unit = {
    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)

    val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC), ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName(StreamingKafka.getClass.getName)

    val group:String = "gps_consum_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",// earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sparkContext:SparkContext = sparkSession.sparkContext

//    sparkContext.setLogLevel("WARN")

    val streamingContext:StreamingContext = new StreamingContext(sparkContext, Seconds(1))

    val result = HbaseTools.getStreamingContextFromHBase(streamingContext, kafkaParams, topics, group, "(.*)gps_topic")
    result.foreachRDD(eachRdd => {
      if (!eachRdd.isEmpty()){
        eachRdd.foreachPartition(eachPartition =>{
          val connection: Connection = HBaseUtil.getConnection
          val jedis: Jedis = JedisUtil.getJedis
          //判断表是否存在，如果不存在就进行创建
          val admin: Admin = connection.getAdmin
          if(!admin.tableExists(TableName.valueOf(Constants.HTAB_GPS))){
            val htabgps = new HTableDescriptor(TableName.valueOf(Constants.HTAB_GPS))
            htabgps.addFamily(new HColumnDescriptor(Constants.DEFAULT_FAMILY))
            admin.createTable(htabgps)
          }
          if(!admin.tableExists(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))){
            val htabgps = new HTableDescriptor(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))
            htabgps.addFamily(new HColumnDescriptor(Constants.DEFAULT_FAMILY))
            admin.createTable(htabgps)
          }
          eachPartition.foreach(record =>{
            //保存到HBase和redis
            val consumerRecords: ConsumerRecord[String, String] = HbaseTools.saveToHBaseAndRedis(connection,jedis, record)
          })
          JedisUtil.returnJedis(jedis)
          connection.close()
        })

        //更新offset
        val offsetRanges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //result.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)  //将offset提交到默认的kafka的topic里面去保存
        for(eachrange <-  offsetRanges){
          val startOffset: Long = eachrange.fromOffset  //起始offset
          val endOffset: Long = eachrange.untilOffset  //结束offset
          val topic: String = eachrange.topic
          val partition: Int = eachrange.partition
          HbaseTools.saveBatchOffset(group,topic,partition+"",endOffset)
        }
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
