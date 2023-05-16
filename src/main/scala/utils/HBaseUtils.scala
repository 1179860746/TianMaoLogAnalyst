package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

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

  /**
   * 以结构化批量插入
   *
   * @param tableName 表名
   * @param data      rowKey -> (colFamily:colQualifier -> data)
   */
  def insertBatchStructuredData(nameSpace: String, tableName: String, data: Map[String, Map[String, String]]): Unit = {
    val table = conn.getTable(TableName.valueOf(nameSpace, tableName))
    val putList = new util.ArrayList[Put]()
    // 遍历每一条数据
    data.foreach { row =>
      val rowKey = row._1
      row._2.foreach { bean =>
        // 0: colFamily, 1: colQualifier
        val colInfo = bean._1.split(":")
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(colInfo(0)), Bytes.toBytes(colInfo(1)), Bytes.toBytes(bean._2))
        putList.add(put)
      }
      if (putList.size() > batchSize) {
        table.put(putList)
        putList.clear()
      }
    }
    // 统一插入
    if (!putList.isEmpty) {
      table.put(putList)
    }
    table.close()
  }

  /**
   * 批量插入原数据
   *
   * @param tableName 表名
   * @param colName   colFamily: colQualifier
   * @param data      rowKey value
   */
  def insertBatchRawData(nameSpace: String, tableName: String, colName: String, data: Map[String, String]): Unit = {
    val table = conn.getTable(TableName.valueOf(nameSpace, tableName))
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

  /**
   * 判断该表是否存在
   *
   * @return
   */
  def isTableExists(namespace: String, table: String): Boolean = {
    val admin: Admin = conn.getAdmin
    val tableName = TableName.valueOf(namespace, table)
    val tableExists = admin.tableExists(tableName)
    admin.close()
    tableExists
  }

}

