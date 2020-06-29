package com.study.helloWord

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/9 11:56
*/

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetWorkWordCound")
    val ssc = new StreamingContext(conf, Seconds(1))

   val lines: ReceiverInputDStream[String] = ssc.socketTextStream("master01", 9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
