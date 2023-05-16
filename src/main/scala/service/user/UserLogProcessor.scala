package service.user

import bean.UserLogBean
import org.apache.spark.streaming.dstream.DStream
import service.user.TotalCalculator.{insertOrUpdate, updateOrCreate}
import utils.{Env, HBaseUtils}

import scala.collection.mutable

object UserLogProcessor {

  private val cofNum: Int = 14
  private val regex0_3: String = "[0-3]"
  private val regexNum: String = """^\d+$"""
  private val sRDColFamily = "info:"
  private val colName = "info:value"
  private val nameSpace = Env.HBaseTable.nS
  private val rawData = Env.HBaseTable.rD
  private val structuredRawData = Env.HBaseTable.sRD

  def process(ds: DStream[Array[String]]): Unit = {
    // 持久化（HBase）
    storeRawDataToHBase(ds)
    // 数据预处理
    val validDs = preProcess(ds)
    // 持久化（HBase）
    // storeStructuredDataToHBase(validDs)
    // 1.每15s为一个窗口统计交易量，将最大值更新在数据库中
    windowCount(validDs)
    // 2.统计用户行为（商家）
    // 3.统计用户行为（品牌）
    totalCount(validDs)
  }

  /**
   * 切割字符串，剔除异常值，长度异常
   *
   * @param ds 原数据
   * @return
   */
  private def preProcess(ds: DStream[Array[String]]): DStream[Array[String]] =
    ds.flatMap { bean =>
      bean.map(_.split("[,|:]"))
        .map(myFilter)
        .filter(_.length == cofNum)
    }

  /**
   * 异常值处理
   *
   * @param strs 已切割数据
   * @return
   */
  private def myFilter(strs: Array[String]): Array[String] = {
    // 内容异常：最后一项actionType:[0, 3]
    if (!strs(strs.length - 1).matches(regex0_3))
      return Array()
    // 内容异常：非数字
    for (i <- 1 until strs.length - 2 by 2)
      if (!strs(i).matches(regexNum)) return Array()
    strs
  }

  /**
   * * 以timestamp，info:value:data格式存储数据到HBase
   *
   * @param values 仅分行的原数据
   */
  private def storeRawDataToHBase(values: DStream[Array[String]]): Unit = {
    values.foreachRDD(
      _.foreachPartition { pRdd =>
        // row为单位
        pRdd.map { items =>
          var i = 0
          val dataMap = items.map { value =>
            val timeStamp = System.currentTimeMillis() + "-" + i
            i += 1
            timeStamp -> value
          }.toMap
          HBaseUtils.insertBatchRawData(nameSpace, rawData, colName, dataMap)
        }
      }
    )
  }

  /**
   * * 以rowKey：timestamp，value：按列切割后的格式化方式存储数据到HBase
   *
   * @param values 预处理后的数据
   */
  private def storeStructuredDataToHBase(values: DStream[Array[String]]): Unit = {
    values.foreachRDD { rdd =>
      rdd.foreachPartition { pRdd =>
        var i = 0
        // row为单位
        val batchData = pRdd.map { items =>
          // 数据项为单位
          val value = items.grouped(2)
            .map { case Array(key, value) =>
              sRDColFamily + key -> value
            }.toMap
          // 防止timestamp相同
          val timeStamp = System.currentTimeMillis() + "-" + i
          i += 1
          timeStamp -> value
        }.toMap
        HBaseUtils.insertBatchStructuredData(nameSpace, structuredRawData, batchData)
      }
    }
  }


  private def windowCount(values: DStream[Array[String]]): Unit = {
    val beans = values.map(strs => {
      UserLogBean(strs(7), strs(9), strs(11))
    })
    WindowCalculator.countActionByWindowDur(beans)
  }

  private def totalCount(values: DStream[Array[String]]): Unit = {
    values.foreachRDD(
      _.foreachPartition(pRdd => {
        val merchant: mutable.Map[String, Array[Long]] = mutable.Map()
        val brand: mutable.Map[String, Array[Long]] = mutable.Map()
        pRdd.foreach(strs => {
          val bean = (strs(7), strs(9), strs(11))
          updateOrCreate(bean._1, bean._3, merchant)
          updateOrCreate(bean._2, bean._3, brand)
        })
        insertOrUpdate(Env.MySQLTable.mAC, merchant.toMap)
        insertOrUpdate(Env.MySQLTable.bAC, brand.toMap)
        merchant.clear()
        brand.clear()
      })
    )
  }
}
