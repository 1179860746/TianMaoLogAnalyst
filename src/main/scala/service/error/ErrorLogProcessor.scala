package service.error

import utils.{Env, Global, MyKafkaUtils}

object ErrorLogProcessor {

  def process(strs: Array[String]): Unit = {
    strs.foreach { str =>
      MyKafkaUtils.send(Global.producer, Env.KafkaPConf.ERROR_TOPIC, str)
    }
  }
}
