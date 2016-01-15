package com.github.spark.camus

import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}
import com.github.spark.camus.config.ConfigSetting
import com.github.spark.camus.read.Reader
import com.github.spark.camus.write.Write2hdfs
import com.github.spark.camus.outputDir.DirFormat
import com.github.spark.camus.offset.OffsetOperator
import com.tresata.spark.kafka.KafkaRDD

/**
 * Created by chico on 13/7/15.
 */

object CamusJob {

  def main(args: Array[String]) {

    val configFile = args(0)
    //val delimiter = args(1)
    val delimiter = "\t"
    val skip = args(1).toLong

    val sparkConf = new SparkConf().setAppName("SparkCamusJob")
    val sc = new SparkContext()

    val props = new Properties()
    val config = new ConfigSetting(props, configFile)

    val fromOff = OffsetOperator.getWritedOff(sc, config.currOffPath, config.lastPath, delimiter, config.partNum)
    val endOff = OffsetOperator.getOffsetFromFile(sc, config.offsetDir, delimiter, config.partNum)

    /*
    2015-8-14
    compare the currentOffset and earliestOffset
     */
    val earlyOff = OffsetOperator.getEarliestOff(endOff, config.kc, config.topicPartition)
    val trueFromOff = for ((k, v) <- fromOff) yield {
      val truev = if (fromOff(k) > earlyOff(k)) fromOff(k) else (earlyOff(k) + skip)
      (k, truev)
    }

    val toOff = OffsetOperator.getCurrentOffAndTime(endOff, config.kc, config.topicPartition)
    //val offsets = OffsetOperator.offsetRange(fromOff, toOff)
    val offsets = OffsetOperator.offsetRange(trueFromOff, toOff)

    val rdd = KafkaRDD(
      sc,
      config.topic,
      offsets,
      config.scconf)

    val currentTime = OffsetOperator.getCurrentOffAndTime(endOff, config.kc, config.topicPartition)
      .getOrElse(100, System.currentTimeMillis())
    val outputDir = DirFormat.byCurrentTime(config.outputBaseDir, currentTime)

    config.reader match {
      case "bytebuffer" => Write2hdfs.bytesArrayWriter(rdd.map(Reader.pomToTuple), outputDir)
      case "string" => Write2hdfs.stringWriter(rdd.map(Reader.pomToString), outputDir)
      case _ => println("#### Wrong reader")
    }

    OffsetOperator.offsetFlush(sc, config.currOffPath, toOff, delimiter)

  }

}

