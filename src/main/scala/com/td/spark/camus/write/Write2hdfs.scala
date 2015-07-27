package com.td.spark.camus.write

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Created by chico on 13/7/15.
 */
object Write2hdfs {

  def bytesArrayWriter(input: RDD[Array[Byte]], outputDir: String) = {
    input.map(bytesArray => (NullWritable.get(), new BytesWritable(bytesArray)))
      .saveAsSequenceFile(outputDir, Some(classOf[org.apache.hadoop.io.compress.GzipCodec]))
  }

  def stringWriter(input: RDD[String], outputDir: String) = {
    input.saveAsTextFile(outputDir)
  }

}
