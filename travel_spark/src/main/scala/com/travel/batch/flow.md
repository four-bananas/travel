## 处理虚拟车站数据整体流程

- 获取order表所有数据 from hbase

- 取到 orderRDD 转换称为 orderDF 并注册成 TempView -> orderDF

- 注册自定义 udf 并执行查询（使用h3算法计算出hash值）

- 生成新的 TempView -> order_grid