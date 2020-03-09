package com.sev7e0.travel.sink;

import com.travel.common.Constants;
import com.travel.common.HBaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



public class HBaseOrderSink extends RichSinkFunction<String> {

	private Connection connection;
	private Admin admin;

	@Override
	public void open(Configuration parameters) throws Exception {
		connection = HBaseUtil.getConnection();
		admin = connection.getAdmin();
		if(!admin.tableExists(TableName.valueOf(Constants.HTAB_GPS))){
			HBaseUtil.createTable(connection,Constants.HTAB_GPS,Constants.DEFAULT_FAMILY);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		admin.close();
		connection.close();
	}

	@Override
	public void invoke(String value, Context context) throws Exception {
		String[] values = value.split("\t");
		if (values.length == 24 && !value.contains("dwv_order_make_haikou")){
			String rowkey = values[0] + "_" + values[13].replaceAll("-", "") + values[14].replaceAll(":", "");
			Put put = new Put(Bytes.toBytes(rowkey));
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"ORDER_ID".getBytes(),values[0].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PRODUCT_ID".getBytes(),values[1].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"CITY_ID".getBytes(),values[2].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DISTRICT".getBytes(),values[3].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"COUNTY".getBytes(),values[4].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TYPE".getBytes(),values[5].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"COMBO_TYPE".getBytes(),values[6].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TRAFFIC_TYPE".getBytes(),values[7].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PASSENGER_COUNT".getBytes(),values[8].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DRIVER_PRODUCT_ID".getBytes(),values[9].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"START_DEST_DISTANCE".getBytes(),values[10].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"ARRIVE_TIME".getBytes(),values[11].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DEPARTURE_TIME".getBytes(),values[12] .getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PRE_TOTAL_FEE".getBytes(),values[13].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"NORMAL_TIME".getBytes(),values[14].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"BUBBLE_TRACE_ID".getBytes(),values[15].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PRODUCT_1LEVEL".getBytes(),values[16].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DEST_LNG".getBytes(),values[17].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DEST_LAT".getBytes(),values[18].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"STARTING_LNG".getBytes(),values[19].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"STARTING_LAT".getBytes(),values[20].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"YEAR".getBytes(),values[21].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"MONTH".getBytes(),values[22].getBytes());
			put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DAY".getBytes(),values[23].getBytes());
			Table table = connection.getTable(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER));
			table.put(put);
		}

	}
}
