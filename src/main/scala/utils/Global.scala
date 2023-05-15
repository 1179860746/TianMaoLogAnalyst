package utils

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object Global {
  private val sc = new SparkConf()
    .setMaster(Env.SparkConf.MASTER)
    .setAppName(Env.SparkConf.APP_NAME)
  val ssc = new StreamingContext(sc, Milliseconds(Env.SparkConf.DURATION))
}

