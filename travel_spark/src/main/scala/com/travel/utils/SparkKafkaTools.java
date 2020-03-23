package com.travel.utils;

import com.travel.common.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;
import java.util.regex.Pattern;

public class SparkKafkaTools {

	/**
	 * 手动管理offset 保存到hbase
	 * @param recordRDD rdd
	 * @param kafkaDS DStream
	 * @param group group
	 */
	public static void saveOffsetToHBase(RDD<ConsumerRecord<String,String>> recordRDD,InputDStream<ConsumerRecord<String, String>> kafkaDS,String group){
		OffsetRange[] offsetRanges = ((HasOffsetRanges) recordRDD).offsetRanges();
		((CanCommitOffsets)kafkaDS).commitAsync(offsetRanges);
		for (OffsetRange offsetRange : offsetRanges){
			long untilOffset = offsetRange.untilOffset();
			String topic = offsetRange.topic();
			int partition = offsetRange.partition();
			HbaseTools.saveBatchOffset(group,topic,String.valueOf(partition),untilOffset);
		}
	}

	/**
	 * 构建Spark Kafka source
	 * @param context StreamingContext
	 * @param topics topic
	 * @param customProp 可选配置文件
	 * @param offset 指定offset
	 * @return kafka InputDStream
	 */
	public static InputDStream<ConsumerRecord<String, String>> buildSourceFromKafka(StreamingContext context,
																					Pattern topics,
																					Map<String,Object> customProp,
																					java.util.Map<TopicPartition, Long> offset){
		if (Objects.nonNull(offset) && !offset.isEmpty()){
			ConsumerStrategy<String, String> matchPattern = ConsumerStrategies.SubscribePattern(topics, buildKafkaProperties(customProp), offset);
			return KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent(), matchPattern);
		}
		ConsumerStrategy<String, String> matchPattern = ConsumerStrategies.SubscribePattern(topics, buildKafkaProperties(customProp));
		return KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent(), matchPattern);
	}

	/**
	 * 获取kafka配置文件
	 * @return 返回map
	 */
	public static Map<String, Object> buildKafkaProperties(){
		return buildKafkaProperties(null);
	}

	/**
	 * 获取kafka配置文件
	 * @param customProp 自定义配置文件
	 * @return
	 */
	public static Map<String, Object> buildKafkaProperties(Map<String, Object> customProp){
		HashMap<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", ConfigUtil.getConfig("bootstrap.servers","spark01:9092"));
		props.put("zookeeper.connect", ConfigUtil.getConfig("zookeeper.connect","spark01:2181"));
		props.put("group.id", ConfigUtil.getConfig("group-id","default-group"));
		props.put("key.deserializer", ConfigUtil.getConfig("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")); //key 反序列化
		props.put("key.serializer", ConfigUtil.getConfig("key.serializer","org.apache.kafka.common.serialization.StringSerializer")) ;//key 反序列化
		props.put("value.deserializer", ConfigUtil.getConfig("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"));
		props.put("value.serializer", ConfigUtil.getConfig("value.serializer","org.apache.kafka.common.serialization.StringSerializer"));
		props.put("auto.offset.reset", ConfigUtil.getConfig("auto.offset.reset","latest"));
		if (Objects.nonNull(customProp)){
			props.putAll(customProp);
		}
		return props;
	}
}
