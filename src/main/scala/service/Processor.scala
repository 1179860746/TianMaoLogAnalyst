package service

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import service.error.ErrorLogProcessor
import service.merchant.MerchantLogProcessor
import service.user.UserLogProcessor

object Processor extends Serializable {

  // 数据标志位置（分流）
  private val userLogFlg = "用户ID:"
  private val merchantLogFlg = "商家ID:"
  private val errorLogFlg = "错误ID:"

  def process(data: InputDStream[ConsumerRecord[String, String]]): Unit = {
    val validData = preProcess(data)
    myDiverter(validData)
  }

  /**
   * 提取Kafka数据中的value
   * 字符串换行切割与剔除长度异常值
   *
   * @param values 原始数据
   * @return
   */
  private def preProcess(values: InputDStream[ConsumerRecord[String, String]]): DStream[String] = {
    values.map(_.value())
      .flatMap(_.trim.split("\\n"))
      .filter(_.length > 4)
  }

  /**
   * 将非用户行为数据分流至其他模块
   * 两种分流方式
   * （1）转成DStream
   * （2）转成目标数据格式，直接使用
   *
   * @param ds 已分行数据（未切割）
   * @return
   */
  private def myDiverter(ds: DStream[String]): Unit = {
    // 根据分类转为K(String)-V(Array[String])结构
    val rs = ds.map { str => (str.substring(0, 4), str) }
      .groupByKey()
      // Tuple2转Array(String, Array[String])
      .mapValues(_.toArray)

    // 转成DStream
    val user = rs.filter(_._1 == userLogFlg).map(_._2)
    UserLogProcessor.process(user)
    val merchant = rs.filter(_._1 == merchantLogFlg).map(_._2)
    MerchantLogProcessor.process(merchant)

    // 转成Array[String]
    rs.map {
      case (`errorLogFlg`, value) => ErrorLogProcessor.process(value)
      case _ =>
    }
  }

}
