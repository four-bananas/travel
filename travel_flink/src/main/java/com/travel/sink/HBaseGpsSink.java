package com.travel.sink;

import com.travel.common.Constants;
import com.travel.common.DateUtils;
import com.travel.common.HBaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.Date;


public class HBaseGpsSink extends RichSinkFunction<String> {

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
		String[] values = value.split(",");
		String driverId = values[0];
		String orderId = values[1];
		String timestamp = values[2];
		String lng = values[3];
		String lat = values[4];
		String rowkey = orderId + "_" + timestamp;
		Put put = new Put(rowkey.getBytes());
		put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"CITYCODE".getBytes(),Constants.CITY_CODE_CHENG_DU.getBytes());
		put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DRIVERID".getBytes(),driverId.getBytes());
		put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"ORDERID".getBytes(),orderId.getBytes());
		put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TIMESTAMP".getBytes(),(timestamp+"").getBytes());
		put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TIME".getBytes(), DateUtils.formateDate(new Date(Long.parseLong(timestamp + "000")),"yyyy-MM-dd HH:mm:ss").getBytes());
		put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"LNG".getBytes(),lng.getBytes());
		put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"LAT".getBytes(),lat.getBytes());
		Table table = connection.getTable(TableName.valueOf(Constants.HTAB_GPS));
		table.put(put);
	}
}
