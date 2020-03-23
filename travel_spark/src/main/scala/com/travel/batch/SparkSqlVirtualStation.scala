package com.travel.batch

import com.travel.bean.HaiKouOrder
import com.travel.common.Constants
import com.travel.sql.VirtualStationSQL
import com.travel.utils.{HbaseTools, SparkUtils}
import com.uber.h3core.H3Core
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.DataTypes
import org.locationtech.jts.geom.Point
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object SparkSqlVirtualStation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    //使用KryoSerializer序列化提升性能
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    conf.setMaster("local[1]").setAppName(SparkSqlVirtualStation.getClass.getName)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")
    val configuration = HBaseConfiguration.create()
    val scan = new Scan
    scan.addFamily(Bytes.toBytes(Constants.DEFAULT_FAMILY))
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("ORDER_ID"))
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("CITY_ID"))
    //获取经度
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LNG"))
    //获取纬度
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LAT"))

    configuration.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(scan))
    configuration.set(TableInputFormat.INPUT_TABLE, Constants.HTAB_HAIKOU_ORDER)

    /**
     * 通过Spark API加载HBase数据
     */
    val hbaseRdd = sparkContext.newAPIHadoopRDD(configuration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    import sparkSession.implicits._
    //分区遍历数据
    val haikouOrderRdd = hbaseRdd.mapPartitions(partation => {
      val orders = partation.map(result => {
        val value = result._2
        val orderId = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("ORDER_ID")))
        val cityId = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("CITY_ID")))
        val startingLng = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LNG")))
        val startingLat = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LAT")))
        //构建新的order对象
        HaiKouOrder(orderId, cityId, startingLng, startingLat)
      })
      orders
    })

    val haikouDF = haikouOrderRdd.toDF()

    haikouDF.createOrReplaceTempView("haikou_view")

    sparkSession.udf.register("getH3", new UDF3[String, String, Int, Long] {
      override def call(t1: String, t2: String, t3: Int): Long = {
        H3Core.newInstance().geoToH3(t1.toDouble, t2.toDouble, t3)
      }
    }, DataTypes.LongType)

    val gridDF = sparkSession.sql(VirtualStationSQL.h3)

    gridDF.createOrReplaceTempView("order_grid")

    val virtualRDD = sparkSession.sql(VirtualStationSQL.joinSql).rdd

    // 获取市区地图的经纬度的边界值，并进行广播

    val value = SparkUtils.broadCastDistrictValue(sparkSession)

    val finalRow: RDD[mutable.Buffer[Row]] = virtualRDD.mapPartitions(partition => {
      import org.geotools.geometry.jts.JTSFactoryFinder
      val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      val reader = new WKTReader(geometryFactory)
      val wktBroadcast = SparkUtils.changeDistictToPolygon(value, reader)
      partition.map(row => {
        //从每一个row中取出经纬度
        val lng = row.getAs[String]("starting_lng")
        val lat = row.getAs[String]("starting_lat")
        //构建数据点
        val pointStr = "POINT(" + lng + " " + lat + ")"
        val point = reader.read(pointStr).asInstanceOf[Point]
        val rows: mutable.Buffer[Row] = wktBroadcast.map(wkt => {
          //判断是够在当前的区域中
          if (wkt._2.contains(point)) {
            val fields = row.toSeq.toArray ++ Seq(wkt._1.getName)
            Row.fromSeq(fields)
          } else {
            null
          }
        }).filter(null != _)
        rows
      })
    })

    val flatFinalRow:RDD[Row] = finalRow.flatMap(x => x)

    HbaseTools.saveOrWriteData(configuration,flatFinalRow,Constants.VIRTUAL_STATION)

  }

}
