# TianMaoLogAnalyst

## 需求：  
 （1）数据经过预处理后持久化至HBase中  
 （2）实时更新用户对商家与品牌数量至MySQL中  
 （3）最高成交量的15s

## 集群:  
共有两台计算机：Computer1负责实时数据分析与数据持久化，Computer2负责模拟数据采集与离线数据分析。  
因此，组件分布如下：Hadoop、HBase、Spark与MySQL部署在Computer1上，Hive、Flume部署在Compute
r2上。
两台计算机在同一网络环境下通过SSH远程通信，访问MySQl与HDFS数据。



![集群](/Users/zwt/Downloads/集群.png)

## 实时处理模块

![实时数据处理模块流程图](..%2F..%2FDownloads%2F%E5%AE%9E%E6%97%B6%E6%95%B0%E6%8D%AE%E5%A4%84%E7%90%86%E6%A8%A1%E5%9D%97.jpg)