package com.github.spark.camus.outputDir

import java.text.SimpleDateFormat

/**
 * Created by chico on 13/7/15.
 */
object DirFormat {

  def dateFormat(date: Long) = {
    val df = new SimpleDateFormat("yyyy/MM/dd/HH-mm")
    df.format(date)
  }

  def byCurrentTime(baseDir: String, currentTime: Long) = {
    baseDir + dateFormat(currentTime)
  }

  // todo by receiveTime
}
