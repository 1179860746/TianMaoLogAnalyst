package app

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContextState
import service.Processor
import utils.Global.ssc
import utils.{Env, HBaseUtils, MyKafkaUtils}

import java.net.URI

object Start extends App {

  init()
  private val rawData = MyKafkaUtils.createConsumer(ssc)
  Processor.process(rawData)
  start()
  stop()

  private def init(): Unit = {
    HBaseUtils.init()
  }

  private def start(): Unit = {
    ssc.start()
  }

  /**
   * 在HDFS中./spark/TianMao下创建文件夹stopJob可停止sparkStreaming任务
   */
  private def stop(): Unit = {
    val fileSystem = FileSystem.get(new URI(Env.SparkConf.hdfsURI), new Configuration())
    new Thread {
      // 保持监听
      while (true) {
        Thread.sleep(10000)
        if (
          ssc.getState() == StreamingContextState.ACTIVE
            &&
            fileSystem.exists(new Path(Env.SparkConf.stopFlgFilePath))
        ) {
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }.start()
  }
}
