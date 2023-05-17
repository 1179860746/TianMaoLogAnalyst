package utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

import java.util.Properties

object MyKafkaUtils {

  private val consumerProperties: Map[String, String] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Env.KafkaCConf.BOOTSTRAP,
    ConsumerConfig.GROUP_ID_CONFIG -> Env.KafkaCConf.GROUP_ID,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> Env.KafkaCConf.KEY_DESERIALIZER,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> Env.KafkaCConf.VALUE_DESERIALIZER,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> Env.KafkaCConf.AUTO_OFFSET_RESET,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> Env.KafkaCConf.ENABLE_AUTO_COMMIT
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

  private val producerProperties: Properties = new Properties()
  Map[String, String](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Env.KafkaPConf.BOOTSTRAP,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> Env.KafkaPConf.KEY_SERIALIZER,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> Env.KafkaPConf.VALUE_SERIALIZER,
    ProducerConfig.ACKS_CONFIG -> Env.KafkaPConf.ACK,
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> Env.KafkaPConf.ENABLE_IDEMPOTENCE
  )
  // errorLog
  private val errorTopic = Env.KafkaPConf.ERROR_TOPIC

  /**
   * 创建Producer
   */
  def createProducer(): KafkaProducer[String, String] = new KafkaProducer[String, String](producerProperties)

  /**
   *
   * @param producer 生产者对象
   * @param topic    主题
   * @param value    数据
   */
  def send(producer: KafkaProducer[String, String], topic: String, value: String): Unit = {
    topic match {
      case `errorTopic` => producer.send(new ProducerRecord[String, String](errorTopic, value))
      case _ =>
    }

  }
}
