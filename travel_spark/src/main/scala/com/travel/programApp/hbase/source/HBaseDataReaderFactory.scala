package com.travel.programApp.hbase.source

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType

class HBaseDataReaderFactory(hbaseTableName: String, hbaseTableSchema: String, sparkSqlTableSchema:String,supportsFilters: Array[Filter], requiredSchema: StructType)
     extends DataReaderFactory[Row]{
  override def createDataReader(): DataReader[Row] = {
    new HBaseCustomDataReader(hbaseTableName, hbaseTableSchema, sparkSqlTableSchema,supportsFilters, requiredSchema)
  }
}
