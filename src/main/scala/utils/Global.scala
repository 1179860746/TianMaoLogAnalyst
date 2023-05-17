package utils

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object Global {
  private val sc = new SparkConf()
    .setMaster(Env.SparkConf.MASTER)
    .setAppName(Env.SparkConf.APP_NAME)
  val ssc: StreamingContext = StreamingContext.getActiveOrCreate(
    Env.SparkConf.checkpointPath,
    () => {
      new StreamingContext(sc, Milliseconds(Env.SparkConf.DURATION))
    }
  )

  val producer: KafkaProducer[String, String] = MyKafkaUtils.createProducer()
}

