package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import java.util

object HBaseUtils {

  private var conn: Connection = _

  private val conf: Configuration = HBaseConfiguration.create()

  def init(): Connection = {
    if (conn == null || conn.isClosed) {
      synchronized {
        conn = ConnectionFactory.createConnection(conf)
      }
    }
    conn
  }

  def getConnection: Connection = init()

  def close(): Unit = if (conn != null && !conn.isClosed) conn.close()

  private val batchSize = Env.HBaseConf.putBatch.toInt

  def insertBatchData(tableName: String, colName: String, data: Map[String, String]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val colInfo: Array[String] = colName.split(":")
    val family = colInfo(0)
    val qualifier = colInfo(1)
    val putList = new util.ArrayList[Put]()
    data.foreach(bean => {
      val rowKey = bean._1
      val values = bean._2
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(values))
      putList.add(put)
      if (putList.size() > batchSize) {
        table.put(putList)
        putList.clear()
      }
    })
    // 统一插入
    if (!putList.isEmpty) {
      table.put(putList)
    }
    table.close()
  }

}

