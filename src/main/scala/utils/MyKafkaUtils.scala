package utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

object MyKafkaUtils {

  private val consumerProperties: Map[String, String] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Env.KafkaCConf.BOOTSTRAP,
    ConsumerConfig.GROUP_ID_CONFIG -> Env.KafkaCConf.GROUP_ID,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> Env.KafkaCConf.KEY_DESERIALIZER,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> Env.KafkaCConf.VALUE_DESERIALIZER
  )
  private val topics = Env.KafkaCConf.TOPICS

  /**
   *
   * @param ssc              sparkStreaming环境StreamingContext
   * @param locationStrategy 存储策略
   * @return Kafka数据流
   */
  def createConsumer(ssc: StreamingContext, locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream(
      ssc,
      locationStrategy,
      ConsumerStrategies.Subscribe[String, String](topics, consumerProperties),
      new PerPartitionConfig() {
        override def maxRatePerPartition(topicPartition: TopicPartition): Long = Env.KafkaCConf.PARTITION_MAX_MSG_NUM
      }
    )
  }

  private val producerProperties: Map[String, String] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Env.KafkaPConf.BOOTSTRAP,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> Env.KafkaPConf.KEY_SERIALIZER,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> Env.KafkaPConf.VALUE_SERIALIZER
  )
  private val producerTopics = Env.KafkaPConf.TOPICS

  // TODO:
  def createProducer(): Unit = {

  }

}
