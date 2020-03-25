package com.travel.programApp.hbase.source

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer
/**
 * 注意：{@link SupportsPushDownFilters }
 * 1. 下推条件必须满足数据类型 比如：string 不能判断大小
 * 2. short and bytes 比较大小必须使用显示转换 where id case (见spark ParquetFilter源码)
 */
class HBaseCustomDataSourceReader
  (hbaseTableName: String, sparkSqlTableSchema: String, hbaseTableSchema: String)
    extends DataSourceReader with SupportsPushDownFilters with SupportsPushDownRequiredColumns{

  var supportsFilters = Array.empty[Filter]
  var requiredSchema:StructType = null

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val supported = ListBuffer.empty[Filter]
    val unsupported = ListBuffer.empty[Filter]

    /**
     * 仅仅把等于大于大于等于，小于小于等于下推
     */
    filters.foreach{
      case filter: EqualTo => supported +=filter
      case filter: GreaterThan=> supported +=filter
      case filter: GreaterThanOrEqual=> supported +=filter
      case filter: LessThan => supported +=filter
      case filter: LessThanOrEqual => supported +=filter
      case filter => unsupported +=filter
    }
    this.supportsFilters = supported.toArray[Filter]
    unsupported.toArray
  }

  override def pushedFilters(): Array[Filter] = supportsFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = this.requiredSchema = requiredSchema

  override def readSchema(): StructType = {
    if(requiredSchema != null){
      return requiredSchema
    }
    StructType.fromDDL(sparkSqlTableSchema)
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import scala.collection.JavaConverters._
    Seq(
      new HBaseDataReaderFactory(hbaseTableName,hbaseTableSchema,sparkSqlTableSchema,supportsFilters,requiredSchema).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }
}
