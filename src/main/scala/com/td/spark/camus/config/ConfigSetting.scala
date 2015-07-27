package com.td.spark.camus.config

import com.tresata.spark.kafka.SimpleConsumerConfig
import java.util.Properties
import kafka.common.TopicAndPartition
import kafka.utils.VerifiableProperties
import org.apache.spark.streaming.kafka.KafkaCluster

/**
 * Created by chico on 13/7/15.
 */
object ConfigSetting {

  def apply(originalProps: Properties, configFile: String): ConfigSetting =
    new ConfigSetting(originalProps, configFile)


  def systemProperty: Unit = {
    System.setProperty("spark.hadoop.mapred.output.compress", "true")
    System.setProperty("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
  }

}

class ConfigSetting(val props: Properties, val configFile: String) {

  val vprops = new VerifiableProperties(props)
  props.load(this.getClass.getResourceAsStream(configFile))

  val lastPath = vprops.getString("lastPath")

  val currOffPath = vprops.getString("currOffPath")

  val topic = vprops.getString("topic")

  val brokerPort = vprops.getInt("brokerPort", 9092)

  val scconf = SimpleConsumerConfig(getConsumerConfig(brokerPort))

  val reader = vprops.getString("reader")

  val outputBaseDir = vprops.getString("outputBaseDir")

  val partNum = vprops.getInt("partNum")

  val kafkaParams = Map(
    "metadata.broker.list" -> vprops.getString("metadata.broker.list"), //10.10.32.94:9092,10.10.32.95:9092,10.10.32.96:9092,10.10.32.97:9092"
    "auto.offset.reset" -> vprops.getString("auto.offset.reset") //"largest"
  )

  def getConsumerConfig(port: Int): Properties = {

    val props = new Properties()
    props.put(
      "metadata.broker.list",
      s"10.10.32.94:9092,10.10.32.95:9092,10.10.32.96:9092,10.10.32.97:9092,172.16.16.5:9092,172.16.16.6:9092,172.16.16.7:9092")
    props.put("fetch.message.max.bytes", "31457280")
    props
  }

  val topicPartition = for (i <- 0 until partNum.toInt) yield {
    TopicAndPartition(topic, i)
  }

  val kc = new KafkaCluster(kafkaParams)

  val offsetDir = vprops.getString("offsetDir")

  vprops.verify()

}

