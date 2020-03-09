package com.travel.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class GpsRedisMapper implements RedisMapper<Tuple2<String,String>> {
	@Override
	public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.LPUSH);
	}

	@Override
	public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
		return stringStringTuple2.f0;
	}


	@Override
	public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
		return stringStringTuple2.f1;
	}


}
