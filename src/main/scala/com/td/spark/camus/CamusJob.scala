package com.td.spark.camus

import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}
import com.td.spark.camus.config.ConfigSetting
import com.td.spark.camus.read.Reader
import com.td.spark.camus.write.Write2hdfs
import com.td.spark.camus.outputDir.DirFormat
import com.td.spark.camus.offset.OffsetOperator
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

    /*val url =
      "http://10.10.32.120:9981/job/trigger?id=Pb2ParquetHour&t=" +
      DirFormat.dateFormat(currentTime) +
      "&p=" +
      outputDir
    scala.io.Source.fromURL(url).getLines()*/

  }

}

object TAlogCamusJob {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("talog-SparkCamusJob")
    val sc = new SparkContext()

    val props = new Properties()
    val config = new ConfigSetting(props, "/talogConfig.properties")

    val fromOff = OffsetOperator.getWritedOff(sc, config.currOffPath, config.lastPath, "  ", config.partNum)
    val endOff = OffsetOperator.getOffsetFromFile(sc, config.offsetDir, "\t", config.partNum)
    val toOff = OffsetOperator.getCurrentOffAndTime(endOff, config.kc, config.topicPartition)
    val offsets = OffsetOperator.offsetRange(fromOff, toOff)

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

    OffsetOperator.offsetFlush(sc, config.currOffPath, toOff)

    val url =
      "http://10.10.32.120:9981/job/trigger?id=Pb2ParquetHour&t=" +
      DirFormat.dateFormat(currentTime) +
      "&p=" +
      outputDir
    scala.io.Source.fromURL(url).getLines()

  }

}

object ADlogCamusJob {

  def main(args: Array[String]) {

    val skip = args(0).toLong

    val sparkConf = new SparkConf().setAppName("ad-SparkCamusJob")
    val sc = new SparkContext()

    val props = new Properties()
    //val config = new ConfigSetting(props, "/config.properties.postman.3")
    val config = new ConfigSetting(props, "/adConfig.properties")
    //val config = new ConfigSetting(props, "/adConfigTemp.properties")

    val fromOff = OffsetOperator.getWritedOff(sc, config.currOffPath, config.lastPath, "  ", config.partNum)
    //val fromOff = OffsetOperator.getWritedOff(sc, config.currOffPath, config.lastPath, "\t", config.partNum)
    val endOff = OffsetOperator.getOffsetFromFile(sc, config.offsetDir, "\t", config.partNum)

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

    OffsetOperator.offsetFlush(sc, config.currOffPath, toOff)

    val url =
      "http://10.10.32.120:9981/job/trigger?id=Json2ParquetHour&t=" +
      DirFormat.dateFormat(currentTime) +
      "&p=" +
      outputDir
    scala.io.Source.fromURL(url).getLines()

    /*
    2015-08-11
    read sequenceFile[NullWritable,BytesWritable]
     */
    /*import org.apache.hadoop.io._
    //val in = args(4)
    val in = "/extract/chico/sequence/"
    val res = sc.sequenceFile[NullWritable, BytesWritable](in).flatMap{
      case (key, value) =>
        val len = value.getLength
        val bytes = value.getBytes.slice(0, len)
        /*try {
          val p = EventPackage.Package.parseFrom(bytes)
          val log = decode(p)
          Some( new GenericRow(log).asInstanceOf[Row] )
        } catch { case e : Exception => None }*/
        try {
          Some(new String(bytes, "UTF8"))
        } catch {
          case e : Exception => None
        }
    //}.coalesce(fn.toInt,false)
    }.coalesce(8,false)*/


  }

}

