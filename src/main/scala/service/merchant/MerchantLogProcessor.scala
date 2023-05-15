package service.merchant

import org.apache.spark.streaming.dstream.DStream

object MerchantLogProcessor {
  def process(ds: DStream[Array[String]]): Unit = {}
}
