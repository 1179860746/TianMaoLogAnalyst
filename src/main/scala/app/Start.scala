package app

import service.Processor
import utils.Global.ssc
import utils.{Global, HBaseUtils, KafkaConsumerFactory}

object Start extends App {

  init()
  private val rawData = KafkaConsumerFactory.createConsumer(ssc)
  Processor.process(rawData)
  start()
  stop()

  private def init(): Unit = {
    // 添加监听器
    // Global.ssc.addStreamingListener(new MyListener)
    HBaseUtils.init()
  }

  private def start(): Unit = {
    if (ssc != null) {
      ssc.start()
      ssc.awaitTermination()
    } else {
      throw new Exception("ssc hasn't been initialized")
    }
  }

  private def stop(): Unit = {
    if (ssc != null) {
      // 关闭HBase连接
      HBaseUtils.close()
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    } else {
      throw new Exception("ssc hasn't been initialized")
    }
  }
}
