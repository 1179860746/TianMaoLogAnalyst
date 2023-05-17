package utils

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, ResultSet}
import java.util.Properties
import javax.sql.DataSource

object JDBCUtils {

  // 初始化连接池
  private val dataSource: DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", Env.MySQLConf.DRIVER)
    properties.setProperty("url", Env.MySQLConf.URL)
    properties.setProperty("username", Env.MySQLConf.USER_NAME)
    properties.setProperty("password", Env.MySQLConf.PASSWORD)
    properties.setProperty("maxActive", Env.MySQLConf.MAX_ACTIVE)
    DruidDataSourceFactory.createDataSource(properties)
  }

  // 获取MySQL连接
  def getConnection: Connection = dataSource.getConnection

  /*
  def insertBatch(conn: Connection, tableName: String, values: Map[String, String]): Unit = {
    val sql = new StringBuilder("insert into " + tableName + " VALUES ")
    values.foreach(bean => {
      sql.append("(\'" + bean._1 + "\',\'" + bean._2 + "\'),")
    })
    sql.deleteCharAt(sql.length - 1).append(";")
    println(sql)
    val stmt = conn.prepareStatement(sql.toString())
    stmt.execute()
    conn.commit()
    stmt.close()
  }
   */

  /**
   * insert或者update
   *
   * @param conn   Connection对象
   * @param sql    执行的sql语句
   * @param params 数据数组
   */
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Unit = {
    try {
      conn.setAutoCommit(false)
      val pstmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i).toString)
        }
      }
      pstmt.executeUpdate()
      conn.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   * insert语句，同主键执行update
   *
   * @param insertSql insert语句
   * @param data      数据数组
   * @param updateSql on duplicate key update逻辑语句
   */
  def executeInsertOrUpdate(conn: Connection, insertSql: String, data: Map[String, Array[Long]], updateSql: String): Unit = {
    try {
      conn.setAutoCommit(false)
      val sql = new StringBuilder(insertSql)
      data.foreach(bean => {
        sql.append("(\'" + bean._1 + "\'," + bean._2(0) + "," + bean._2(1) + "," + bean._2(2) + "," + bean._2(3) + "),")
      })
      sql.deleteCharAt(sql.length - 1)
        .append(updateSql)
      val stmt = conn.prepareStatement(sql.toString())
      stmt.execute()
      conn.commit()
      stmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def query(conn: Connection, sql: String, params: Array[Any]): ResultSet = {
    val pstmt = conn.prepareStatement(sql)
    // 0 .. len -1
    for (i <- params.indices) {
      pstmt.setString(i + 1, params(i).toString)
    }
    println(pstmt.toString)
    val result = pstmt.executeQuery()
    // pstmt.close()
    result
  }

}
