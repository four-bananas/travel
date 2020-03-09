/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sev7e0.travel;

import com.sev7e0.travel.functions.TypeProcessFunction;
import com.sev7e0.travel.sink.GpsRedisMapper;
import com.sev7e0.travel.sink.HBaseGpsSink;
import com.sev7e0.travel.sink.HBaseOrderSink;
import com.sev7e0.travel.utils.FlinkKafkaUtils;
import com.travel.common.ConfigUtil;
import com.travel.common.Constants;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.OutputTag;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingKafkaJob {
	private static final OutputTag<String> chengdu = new OutputTag<>("chengdu");
	private static final OutputTag<String> haikou = new OutputTag<>("haikou");
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		DataStreamSource<String> kafkaSource = FlinkKafkaUtils.buildSourceByPattern(env, "(.*)gps_topic");

		SingleOutputStreamOperator<String> outputStreamOperator = kafkaSource.process(new TypeProcessFunction());

		outputStreamOperator.getSideOutput(chengdu).addSink(new HBaseGpsSink());

		outputStreamOperator.getSideOutput(haikou).addSink(new HBaseOrderSink());


		FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setDatabase(ConfigUtil.getIntegerConfig(Constants.JEDIS_DB,"0"))
			.setHost(ConfigUtil.getStringConfig(Constants.JEDIS_HOST))
			.setPassword(ConfigUtil.getStringConfig(Constants.JEDIS_PASS))
			.setTimeout(3000)
			.setMaxIdle(2000)
			.setMaxTotal(10000)
			.build();

		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig, new GpsRedisMapper());


//		outputStreamOperator.getSideOutput(chengdu).addSink(redisSink);



		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
