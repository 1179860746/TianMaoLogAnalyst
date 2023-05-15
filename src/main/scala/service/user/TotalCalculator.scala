package service.user

import utils.JDBCUtils

import java.sql.Connection
import scala.collection.mutable

object TotalCalculator {

  /**
   * 将数据组一次插入
   * @param tableName 表名
   * @param data      数据
   */
  def insertOrUpdate(tableName: String, data: Map[String, Array[Long]]): Unit = {
    val conn: Connection = JDBCUtils.getConnection
    val insertSql = s"insert into $tableName values"
    val updateSql = {
      """
         |on duplicate key update
         |click = values(click) + click,
         |buy = values(buy) + buy,
         |`add` = values(`add`) + `add`,
         |favor = values(favor) + favor;
         |""".stripMargin
    }
    JDBCUtils.executeInsertOrUpdate(conn, insertSql, data, updateSql)
    conn.close()
  }

  /**
   * ACC累加操作次数，并存储到数据库
   *
   * @param target     商家 / 品牌名
   * @param actionType 操作类型
   * @param map        更新Map
   */
  def updateOrCreate(target: String, actionType: String, map: mutable.Map[String, Array[Long]]): Unit = {
    if (map.keySet.contains(target))
      update(target, actionType, map)
    else {
      map += target -> Array[Long](0.toLong, 0.toLong, 0.toLong, 0.toLong)
      update(target, actionType, map)
    }
  }

  /**
   *
   * @param target     商家 / 品牌
   * @param actionType 操作类型
   * @param map        更新Map
   */
  private def update(target: String, actionType: String, map: mutable.Map[String, Array[Long]]): Unit = {
    actionType match {
      case "0" => map.update(target, Array(map(target)(0) + 1, map(target)(1), map(target)(2), map(target)(3)))
      case "1" => map.update(target, Array(map(target)(0), map(target)(1) + 1, map(target)(2), map(target)(3)))
      case "2" => map.update(target, Array(map(target)(0), map(target)(1), map(target)(2) + 1, map(target)(3)))
      case "3" => map.update(target, Array(map(target)(0), map(target)(1), map(target)(2), map(target)(3) + 1))
    }
  }

}
