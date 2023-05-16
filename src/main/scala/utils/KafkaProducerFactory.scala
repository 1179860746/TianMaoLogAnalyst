package utils

import org.apache.kafka.clients.producer.ProducerConfig

object KafkaProducerFactory {

  private val properties: Map[String, String] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Env.KafkaPConf.BOOTSTRAP,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> Env.KafkaPConf.KEY_SERIALIZER,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> Env.KafkaPConf.VALUE_SERIALIZER
  )
  private val topics = Env.KafkaPConf.TOPICS

  // TODO:
  def createProducer(): Unit = {}
}
