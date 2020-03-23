package com.travel.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;

public class HBaseUtil {

    protected static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    private static Connection connection = null;

    /**
     * 初始化hbase的连接
     *
     * @throws IOException
     */
    private static void initConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
        }
    }

    /**
     * 获得连接
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            initConnection();
        }
        //连接可用直接返回连接
        return connection;
    }

    /**
     *
     *
     * @param tableNameString
     * @param columnFamily
     * @throws IOException
     */
    public static void checkOrCreateTable(Connection connection, String tableNameString, String columnFamily) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString);
        HTableDescriptor table = new HTableDescriptor(tableName);
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        table.addFamily(family);
        //判断表是否已经存在
        if (!admin.tableExists(tableName)) {
            admin.createTable(table);
        }else{
            //如果表已经存在了，判断列族是否存在
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            Arrays.asList(columnFamilies).forEach(des->{
                if(!columnFamily.equals(des.getNameAsString())){
                    try {
                        admin.modifyColumn(tableName, family);
                    } catch (IOException e) {
                        logger.error("modify hbase column error", e);
                    }
                }
            });
        }
        admin.close();
    }

    /**
     * 判断hbase的表是否存在
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean tableExists(String tableName) throws Exception {
        if (connection == null || connection.isClosed()) {
            initConnection();
        }
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return true;
        }
        return false;
    }


    /**
     * 执行hbase预分区策略
     * @param connection hbase连接
     * @param tableName 要创建那些表名
     * @param regionNum 分区数
     * @param dropExistTable 当表名存在时，是否删除原表
     * @throws IOException IOException
     */
    public static void doPreRegion(Connection connection, HashMap<String,String> tableName, Integer regionNum,
                                   Boolean dropExistTable) throws IOException {
        Admin admin = connection.getAdmin();
        for (Map.Entry<String,String> entry : tableName.entrySet()){
            TableName name = TableName.valueOf(entry.getKey());
            HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(entry.getValue());
            hTableDescriptor.addFamily(hColumnDescriptor);
            byte[][] bytes = new byte[regionNum-1][];
            for (int i = 0; i < regionNum-1; i++) {
                String leftPad = StringUtils.leftPad(i + "", 4, "0");
                bytes[i] = Bytes.toBytes(leftPad+"|");
            }
            if (admin.tableExists(name)){
                if (dropExistTable){
                    logger.info("table {} exist, will drop it.", name);
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }else {
                    logger.warn("table {} exist.", entry.getKey());
                    continue;
                }

            }
            admin.createTable(hTableDescriptor, bytes);
            logger.info("create hbase table {} completed!, with columnFamily {} and {} regions.", name,
                entry.getValue(), regionNum);
        }
        admin.close();
    }

    public static Table getTable(String tableName)throws Exception{
        Connection connection = getConnection();
//        Admin admin = connection.getAdmin();
//        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static void savePuts(List<Put> putList,String tableName) throws Exception {
        if (!tableExists(tableName)) {
            HBaseUtil.checkOrCreateTable(getConnection(), tableName, Constants.DEFAULT_FAMILY);
        }
        Table table = HBaseUtil.getTable(tableName);
        table.put(putList);
        table.close();
    }
    /**
     * 获取插入HBase的操作put
     * @param rowKeyString
     * @param familyName
     * @param columnName
     * @param columnValue
     * @return
     */
    public static Put createPut(String rowKeyString, byte[] familyName, String columnName, String columnValue) {
        byte[] rowKey = rowKeyString.getBytes();
        Put put = new Put(rowKey);
        put.addColumn(familyName, columnName.getBytes(), columnValue.getBytes());
        return put;
    }
    /**
     * 获取插入HBase的操作put
     * @param rowKeyString
     * @param familyName
     * @param columns      列
     * @return
     */
    public static Put createPut(String rowKeyString, byte[] familyName, Map<String, String> columns) {
        byte[] rowKey = rowKeyString.getBytes();
        Put put = new Put(rowKey);

        for (Map.Entry<String, String> entry : columns.entrySet()) {
            put.addColumn(familyName, entry.getKey().getBytes(), entry.getValue().getBytes());
        }

        return put;
    }
    /**
     * 打印HBase查询结果
     *
     * @param result
     */
    public static void print(Result result) {
        //result是个四元组<行键，列族，列(标记符)，值>
        byte[] row = result.getRow(); //行键
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : map.entrySet()) {
            byte[] familyBytes = familyEntry.getKey(); //列族
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : familyEntry.getValue().entrySet()) {
                byte[] column = entry.getKey(); //列
                for (Map.Entry<Long, byte[]> longEntry : entry.getValue().entrySet()) {
                    Long time = longEntry.getKey(); //时间戳
                    byte[] value = longEntry.getValue(); //值
                    System.out.println(String.format("行键rowKey=%s,列族columnFamily=%s,列column=%s,时间戳timestamp=%d,值value=%s", new String(row), new String(familyBytes), new String(column), time, new String(value)));
                }
            }
        }

    }

    /**
     * 根据表名开始时间和结束时间查询轨迹
     *
     * @param tableName
     * @param orderId
     * @param startTimestampe
     * @param endTimestampe
     * @return
     */
    public static <T> List<T> getRest(String tableName, String orderId,
                                      String startTimestampe, String endTimestampe, Class<T> clazz) throws Exception {
        Table table = null;
        Scan scanner = null;
        List<T> restList = null;
        try {
            restList = new ArrayList<>();
            table = getTable(tableName);
            String startRowKey = orderId + "_" + startTimestampe;
            String endRowKey = orderId + "_" + endTimestampe;
            scanner = new Scan();

            scanner.setStartRow(startRowKey.getBytes());
            scanner.setStopRow(endRowKey.getBytes());

            try (ResultScanner rs = table.getScanner(scanner)) {

                for (Result r : rs) {
                    //反射形成对象
                    //获取字节码对象类的所有字段属性
                    Field[] fields = clazz.getDeclaredFields();
                    T t = clazz.newInstance();
                    NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Constants.DEFAULT_FAMILY.getBytes());
                    for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                        String colName = Bytes.toString(entry.getKey());
                        String colValue = Bytes.toString(entry.getValue());

                        for (Field field : fields) {
                            String fieldName = field.getName();
                            field.setAccessible(true);
                            if (fieldName.equalsIgnoreCase(colName)) {
                                String fieldType = field.getType().toString();
                                if (fieldType.equalsIgnoreCase("class java.lang.String")) {
                                    field.set(t, colValue);
                                } else if (fieldType.equalsIgnoreCase("class java.lang.Integer")){
                                    field.set(t, Integer.parseInt(colValue));
                                } else if (fieldType.equalsIgnoreCase("class java.lang.Long")){
                                    field.set(t, Long.parseLong(colValue));
                                } else if (fieldType.equalsIgnoreCase("class java.lang.Double")) {
                                    field.set(t, Double.parseDouble(colValue));
                                } else if (fieldType.equalsIgnoreCase("class java.util.Date")) {
                                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                    field.set(t, sdf.format(new Date(Long.parseLong(colValue + "000"))));
                                } else if (fieldType.equalsIgnoreCase("java.lang.Boolean")) {
                                    field.set(t, Boolean.parseBoolean(colValue));
                                } else {
                                    field.set(t, null);
                                }
                            }
                        }
                    }
                    restList.add(t);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return restList;
    }

}
