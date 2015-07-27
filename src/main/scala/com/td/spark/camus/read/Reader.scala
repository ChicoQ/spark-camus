package com.td.spark.camus.read

import java.nio.ByteBuffer
import com.tresata.spark.kafka.PartitionOffsetMessage

/**
 * Created by chico on 13/7/15.
 */
object Reader {

  def readBytes(buffer: ByteBuffer, offset: Int, size: Int): Array[Byte] = {
    val dest = new Array[Byte](size)
    if (buffer.hasArray) {
      System.arraycopy(buffer.array, buffer.arrayOffset() + offset, dest, 0, size)
    } else {
      buffer.mark()
      buffer.get(dest)
      buffer.reset()
    }
    dest
  }

  //private
  def byteBufferToString(bb: ByteBuffer): String = {
    if (bb == null) null
    else {
      val b = new Array[Byte](bb.remaining)
      bb.get(b, 0, b.length)
      new String(b, "UTF8")
    }
  }

  def pomToString = { (pom: PartitionOffsetMessage) =>
    byteBufferToString(pom.message.payload)
  }

  /*
  read protobuf msg from kafka to hdfs
   */

  def pomToTuple = { (pom: PartitionOffsetMessage) =>
    val pl = pom.message.payload
    readBytes(pl, 0, pl.limit)
  }

  /*
  protobuf to parquet
   */
  // todo
  /*implicit val protobufCodecInjection = ProtobufCodec.apply[Package]

  class KafkaSparkRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Package], new ProtobufSerializer)
    }
  }*/


}
