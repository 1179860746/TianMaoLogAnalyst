package utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable
object KafkaConsumerFactory {
  private val properties: mutable.Map[String, String] = mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Env.KafkaCConf.BOOTSTRAP,
    ConsumerConfig.GROUP_ID_CONFIG -> Env.KafkaCConf.GROUP_ID,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> Env.KafkaCConf.KEY_DESERIALIZER,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> Env.KafkaCConf.VALUE_DESERIALIZER
  )
  private val topics = Env.KafkaCConf.TOPICS

  /**
   *
   * @param ssc sparkStreaming环境StreamingContext
   * @param locationStrategy 存储策略
   * @return Kafka数据流
   */
  def createConsumer(ssc: StreamingContext, locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream(
      ssc,
      locationStrategy,
      ConsumerStrategies.Subscribe[String, String](topics, properties),
      new PerPartitionConfig() {
        override def maxRatePerPartition(topicPartition: TopicPartition): Long = Env.KafkaCConf.PARTITION_MAX_MSG_NUM
      }
    )
  }

}
