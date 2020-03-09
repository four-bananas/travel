package com.sev7e0.travel.functions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 对
 */
public class TypeProcessFunction extends ProcessFunction<String, String> {

	private static final OutputTag<String> chengdu = new OutputTag<>("chengdu");
	private static final OutputTag<String> haikou = new OutputTag<>("haikou");

	@Override
	public void processElement(String s, Context context, Collector<String> collector) throws Exception {
		if (s.split(",").length>4){
			context.output(chengdu,s);
		}else {
			context.output(haikou,s);
		}
		collector.collect(s);
	}
}
