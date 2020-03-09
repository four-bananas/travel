package com.travel.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class FlinkKafkaUtils {



	public static DataStreamSource<String> buildSource(StreamExecutionEnvironment env, List<String> topic){
		return env.addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), getKafkaProperties()));
	}
	public static DataStreamSource<String> buildSourceByPattern(StreamExecutionEnvironment env, String matchPattern){
		return env.addSource(new FlinkKafkaConsumer011<>(Pattern.compile(matchPattern), new SimpleStringSchema(), getKafkaProperties()));
	}

	public static Properties getKafkaProperties(){
		return getKafkaProperties(ParameterTool.fromSystemProperties());
	}

	public static Properties getKafkaProperties(ParameterTool parameterTool){
		Properties props = new Properties();
		props.put("bootstrap.servers",parameterTool.get("bootstrap.servers"));
		props.put("zookeeper.connect", parameterTool.get("zookeeper.connect"));
		props.put("group.id", parameterTool.get("group-id"));
		props.put("key.deserializer", parameterTool.get("key.deserializer")); //key 反序列化
		props.put("key.serializer", parameterTool.get("key.serializer")) ;//key 反序列化
		props.put("value.deserializer", parameterTool.get("value.deserializer"));
		props.put("value.serializer", parameterTool.get("value.serializer"));
		props.put("auto.offset.reset", parameterTool.get("auto.offset.reset"));
		props.put("enable.auto.commit", parameterTool.get("enable.auto.commit"));
		return props;
	}

	public static KafkaProducer<String,String> getProducer(Properties properties){
		return new KafkaProducer<>(properties);
	}


}
