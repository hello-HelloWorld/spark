package com.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


//Dstream的数据源---Rdd队列
object QueueRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("queueRdd")
    val ssc = new StreamingContext(conf, Seconds(1))

    //创建Rdd队列
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

    //创建QueueInputDstream
    val inputStream = ssc.queueStream(rddQueue)

    //处理队列中的rdd数据
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    //打印结果
    reducedStream.print()

    //启动计算
    ssc.start()

    //// Create and push some RDDs into
    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
      //通过程序停止StreamContext的运行
      //      ssc.stop()
    }
  }
}
