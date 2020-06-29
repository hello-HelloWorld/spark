package com.study.windowWordCount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/9 14:36
*/

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("./checkPoint")

    val lines = ssc.socketTextStream("master01", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))



    //    val wordCount=pairs.reduceByKey(_+_)

    // 窗口大小 为12s， 12/3 = 4  滑动步长 6S，   6/3 =2
    //    val wordCount = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(6))

    val wordCount = pairs.reduceByKeyAndWindow(_ + _, _ - _, Seconds(12), Seconds(6))

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }


}
