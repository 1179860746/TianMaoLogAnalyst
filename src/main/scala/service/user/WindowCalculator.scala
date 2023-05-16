package service.user

import bean.UserLogBean
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import utils.{Env, Global, JDBCUtils}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 用于计算窗口型需求
 * 1.每15s内最高交易量的商家
 */
object WindowCalculator {

  // 定义一个变量，用于保存最大的满足条件的数据的数量总和
  private val actionType: String = "2"

  // 保存Max数量总和，创建时更新（若为空则赋值0）
  private var mostTran: Int = {
    val conn = JDBCUtils.getConnection
    val selectSql = "select mom_most_tran from mom_most_tran where my_key = 'mom_most_tran'"
    val values = Array[Any]()
    val rs = JDBCUtils.query(conn, selectSql, values)
    if (rs.next()) {
      val r = rs.getInt("mom_most_tran")
      conn.close()
      r
    } else {
      val querySql = "insert into mom_most_tran value('mom_most_tran', '0', '0', '0')"
      JDBCUtils.executeUpdate(conn, querySql, null)
      conn.close()
      0
    }
  }

  /**
   * 求自定义窗口内LogBean.actionType（默认”2“）统计数量的最大值，并存储到数据库
   * @param windowSize 窗口大小
   * @param windowSlide 滑动步长
   */
  def countActionByWindowDur
  (
    ds: DStream[UserLogBean],
    actionType: String = actionType,
    windowSize: Long = Env.SparkConf.WINDOW_SIZE,
    windowSlide: Long = Env.SparkConf.WINDOW_SLIDE
  ): Unit = {
    // 格式化时间
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    val now: Date = new Date()
    val firstTime: String = dateFormat.format(now)
    val timeAfter15Seconds: Date = new Date(now.getTime + windowSize * 1000)
    val lastTime: String = dateFormat.format(timeAfter15Seconds)

    // 定义一个累加器，用于在每个批次中保存满足条件的数据的数量总和
    val tranAcc = Global.ssc.sparkContext.longAccumulator("tranAcc")

    // 统计总数
    ds.window(Seconds(windowSize), Seconds(windowSlide))
      .filter { _.action_type == actionType }
      .map { _ => 1 }
      .reduce { _ + _ }
      .foreachRDD { rdd =>
        val sum = rdd.collect().sum
        tranAcc.add(sum)
        // 判断是否需要插入
        val totalSum = tranAcc.value
        if (totalSum > mostTran) {
          mostTran = totalSum.toInt
          val conn = JDBCUtils.getConnection
          val sql =
            s"""
               |update ${Env.MySQLTable.mMT} set
               |mom_most_tran = ?,
               |time_start = ?,
               |time_end = ?
               |where my_key = 'mom_most_tran';
               |""".stripMargin
          JDBCUtils.executeUpdate(conn, sql, Array[Any](mostTran.toString, firstTime, lastTime))
          conn.close()
        }
        // 每批数据刷新一次，置0
        if (!tranAcc.isZero) {
          tranAcc.reset()
        }
      }
  }

}

