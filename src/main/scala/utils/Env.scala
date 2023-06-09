package utils

import java.util.ResourceBundle

object Env {
  private val kfCConf = ResourceBundle.getBundle("config/kf_c_conf")
  private val kfPConf = ResourceBundle.getBundle("config/kf_p_conf")
  private val mysqlConf = ResourceBundle.getBundle("config/mysql_conf")
  private val mysqlTable = ResourceBundle.getBundle("config/mysql_table")
  private val sparkConf = ResourceBundle.getBundle("config/spark_conf")
  private val hbaseTable = ResourceBundle.getBundle("config/hbase_table")
  private val hbaseConf = ResourceBundle.getBundle("config/hbase_conf")


  object KafkaCConf {
    val BOOTSTRAP: String = kfCConf.getString("bootstrap")
    val GROUP_ID: String = kfCConf.getString("group.id")
    val KEY_DESERIALIZER: String = kfCConf.getString("key.deserializer")
    val VALUE_DESERIALIZER: String = kfCConf.getString("value.deserializer")
    val TOPICS: Seq[String] = kfCConf.getString("topics").split(",").toSeq
    val PARTITION_MAX_MSG_NUM: Long = kfCConf.getString("partitionMaxMsgNum").toLong
    val AUTO_OFFSET_RESET: String = kfPConf.getString("auto.offset.reset")
    ,
    val ENABLE_AUTO_COMMIT: String = kfPConf.getString("enable.auto.commit")
  }

  object KafkaPConf {
    val BOOTSTRAP: String = kfPConf.getString("bootstrap")
    val KEY_SERIALIZER: String = kfPConf.getString("key.serializer")
    val VALUE_SERIALIZER: String = kfPConf.getString("value.serializer")
    val ACK: String = kfPConf.getString("ack")
    val ENABLE_IDEMPOTENCE: String = kfPConf.getString("enable_idempotence")
    val ERROR_TOPIC: String = kfPConf.getString("error.topic")
  }

  object MySQLConf {
    val DRIVER: String = mysqlConf.getString("driver")
    val URL: String = mysqlConf.getString("url")
    val USER_NAME: String = mysqlConf.getString("username")
    val PASSWORD: String = mysqlConf.getString("password")
    val MAX_ACTIVE: String = mysqlConf.getString("max.active")
  }

  object SparkConf {
    val MASTER: String = sparkConf.getString("master")
    val APP_NAME: String = sparkConf.getString("app.name")
    val DURATION: Long = sparkConf.getString("duration.milliseconds").toLong
    val WINDOW_SIZE: Long = sparkConf.getString("window.size").toLong
    val WINDOW_SLIDE: Long = sparkConf.getString("window.slide").toLong
    val checkpointPath: String = sparkConf.getString("checkpoint.path")
    val stopFlgFilePath: String = sparkConf.getString("stop.flag.file.path")
    val hdfsURI: String = sparkConf.getString("hdfs.uri")
  }

  object MySQLTable {
    val rD: String = mysqlTable.getString("raw.data")
    val mMT: String = mysqlTable.getString("mom.most.tran")
    val mAC: String = mysqlTable.getString("merchant.action.count")
    val bAC: String = mysqlTable.getString("brand.action.count")
  }

  object HBaseConf {
    val putBatch: String = hbaseConf.getString("put.batch")
  }

  object HBaseTable {
    val rD: String = hbaseTable.getString("raw.data")
    val nS: String = hbaseTable.getString("namespace")
    val sRD: String = hbaseTable.getString("structured.raw.data")
  }

}
