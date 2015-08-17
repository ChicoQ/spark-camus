package com.td.spark.camus.offset

import kafka.common.TopicAndPartition
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaCluster

/**
 * Created by chico on 13/7/15.
 */
object OffsetOperator {

  def pathFlush(path: String) = {

    val jobConf = new Configuration()
    val hadoopfs = org.apache.hadoop.fs.FileSystem.get(jobConf)
    try {
      hadoopfs.delete(new org.apache.hadoop.fs.Path(path), true)
    } catch {
      case _: Throwable => {}
    }
  }

  def offsetFlush(sc: SparkContext, currOffPath: String, currentOffData: Map[Int, Long]) = {

    pathFlush(currOffPath)

    sc.parallelize(currentOffData.toList, 1)
      .map { case (p, o) => s"$p  $o" }
      .saveAsTextFile(currOffPath)
  }

  def offsetFlush(sc: SparkContext, currOffPath: String, currentOffData: Map[Int, Long], delimiter: String) = {

    pathFlush(currOffPath)

    sc.parallelize(currentOffData.toList, 1)
      .map { case (p, o) => s"$p$delimiter$o" }
      .saveAsTextFile(currOffPath)
  }

  def offsetFlush(sc: SparkContext, currOffPath: String, currentOffData: List[String]) = {

    pathFlush(currOffPath)

    sc.parallelize(currentOffData, 1)
      .saveAsTextFile(currOffPath)
  }

  def readOffset(input: RDD[String]) = {
    input.map { line =>
      val sp = line.split("  ")
      (sp(0).toInt -> sp(1).toLong)
    }.collect.toMap
  }

  def readOffset(input: RDD[String], delimiter: String) = {
    input.map { line =>
      val sp = line.split(delimiter)
      (sp(0).toInt -> sp(1).toLong)
    }.collect.toMap
  }

  def offsetRangeBefore(base: Map[Int, Long], len: Long) = {
    val rangeOut = for ((k, v) <- base) yield {
      (k, (v - len, v))
    }
    rangeOut
  }

  def offsetRangeAfter(base: Map[Int, Long], len: Long) = {
    val rangeOut = for ((k, v) <- base) yield {
      (k, (v, v + len))
    }
    rangeOut
  }

  def offsetRange(writedOff: Map[Int, Long], currentOff: Map[Int, Long]) = {
    val range = for ((k, v) <- writedOff) yield {
      (k, (v, currentOff(k)))
    }
    range
  }

  def getWritedOff(sc: SparkContext, currOffPath: String, lastPath: String, delimiter: String, partNum: Int) = {

    val writedOffData = sc.textFile(currOffPath)
      //.persist(StorageLevel.MEMORY_ONLY_SER_2)
    /*println(s"###### $currOffPath")
    println(s"@@@@ ${writedOffData.collect.mkString(";")}")*/

    writedOffData
      .coalesce(1, true)
      .saveAsTextFile(lastPath + "/" + System.currentTimeMillis())

    val writedOffRdd = writedOffData.map { line =>
      val sp = line.split(delimiter)
      (sp(0).toInt -> sp(1).toLong)
    }


    val writedOff = writedOffRdd.filter{ case (p, o) => p < partNum }
      .collect.toMap

    writedOff
  }

  def getCurrentOffAndTime(writedOff: Map[Int, Long], kc: KafkaCluster, topicPartition: Seq[TopicAndPartition]) = {

    val currentOff = kc.getLatestLeaderOffsets(Set(topicPartition: _*)) match {
      case Left(e) => writedOff
      //case Left(e) => getWritedOff(sc, curr, last)
      case Right(until) => for (item <- until) yield { (item._1.partition, item._2.offset) }
    }
    val currentTime = System.currentTimeMillis()
    val currentOffData = currentOff + (100 -> currentTime)

    currentOffData
  }

  def getEarliestOff(writedOff: Map[Int, Long], kc: KafkaCluster, topicPartition: Seq[TopicAndPartition]) = {

    val earliestOff = kc.getEarliestLeaderOffsets(Set(topicPartition: _*)) match {
      case Left(e) => writedOff // todo
      case Right(until) => for (item <- until) yield { (item._1.partition, item._2.offset) }
    }
    val currentTime = System.currentTimeMillis()
    val currentOffData = earliestOff + (200 -> currentTime)

    currentOffData
  }

  def getStartOff(sc: SparkContext, startOffPath: String, partNum: Int) = {

    val writedOffData = sc.textFile(startOffPath)

    val writedOffRdd = writedOffData.map { line =>
      val sp = line.split("\t")
      (sp(1).toInt -> sp(2).toLong)
    }

    val writedOff = writedOffRdd.filter{ case (p, o) => p < partNum }
      .collect.toMap

    writedOff
  }

  // TODO rewrite, need a new offset data structure, "partition \t offset"
  def offsetOperation(sc: SparkContext, currOffPath: String, lastPath: String, delimiter: String, partNum: Int) = {

    val writedOffData = sc.textFile(currOffPath)

    writedOffData
      .coalesce(1, true)
      .saveAsTextFile(lastPath + "/" + System.currentTimeMillis())

    val writedOffRdd = writedOffData.map { line =>
      val sp = line.split(delimiter)
      (sp(0).toInt -> sp(1).toLong)
    }

    val writedOff = writedOffRdd.filter{ case (p, o) => p < partNum }
      .collect.toMap

    writedOff
  }

  def getOffsetFromFile(sc: SparkContext, filePath: String, delimiter: String, partNum: Int) = {

    val writedOffData = sc.textFile(filePath)

    val writedOffRdd = writedOffData.map { line =>
      val sp = line.split(delimiter)
      (sp(0).toInt -> sp(1).toLong)
    }

    val writedOff = writedOffRdd.filter{ case (p, o) => p < partNum }
      .collect.toMap

    writedOff
  }

}
