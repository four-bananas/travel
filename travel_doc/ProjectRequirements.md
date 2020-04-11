# About Project Requirements

## 出行概览

### 订单信息

主要通过收集实时订单数、乘客数量、累计轨迹点数进行展示

### 订单数量变化

主要通过收集实时订单数计算出数量变化

### 平均行车速度

通过司机上报数据实时统计出平台中车辆的平均行驶速度

## 订单监控

按城市统计展示车辆数和订单数（日、周、月）

## 轨迹监控
通过实时上报的的司机数据进行订单轨迹绘制，同时对当前已经完成的订单生成历史订单，同样点击后支持完整路径展示。
### opentsDB 的虚拟轨迹实现
- 轨迹实时上报位置信息到服务器。
    - 报文包括：cityCode、orderId、driverId、timestamp、lat、lng。
- flume 接收实时数据保存到kafka
    - topic topic_travel_citycode
- 经spark streaming程序处理数据保存到opentsDB
    - 指标: `travel.coordinates.citycode`
    - tag1: orderId = 001
    - tag2: driverId = 002
    - timestamp
    - value: lng,lag
    ```properties
    put travel.coordinates.citycode orderId=001 driverId=002 timestamp 104.08344,30.65588
    ```
- 提供基于opentsDB的虚拟轨迹接口给业务系统使用


## 虚拟车站



## 出行迁徙

## 用户数据

## 热力图

## 系统监控
