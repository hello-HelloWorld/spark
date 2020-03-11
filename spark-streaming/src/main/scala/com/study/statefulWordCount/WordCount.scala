package com.study.statefulWordCount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/9 13:51
*/

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    //创建streamingContext
    val ssc = new StreamingContext(conf, Seconds(1))
    //设置一个checkPoint目录
    ssc.checkpoint(".")

    // 通过StreamingContext来获取master01机器上9999端口传过来的语句
    val lines = ssc.socketTextStream("master01", 9999)

    // 需要通过空格将语句中的单词进行分割DStream[RDD[String]]
    val pairs = lines.flatMap(_.split(" "))

    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // 需要将每一个单词都映射成为一个元组（word,1）
    val words = pairs.map(word => (word, 1))

    // 定义一个更新方法，values是当前批次RDD中相同key的value集合，state是框架提供的上次state的值
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      //计算当前批次相同key的单词总数
      val concurrentCount = values.foldLeft(0)(_ + _)
      //获取上一次保存的单词总数
      val perviousCount = state.getOrElse(0)
      //返回新的单词总数
      Some(concurrentCount + perviousCount)
    }

    //使用updateStateByKey方法,类型参数为状态的类型，后面传入一个更新方法
    val stateDstream = words.updateStateByKey[Int](updateFunc)
    ///输出
    stateDstream.print()
    stateDstream.saveAsTextFiles("hdfs://master01:9000/stateful/", "abc")

    ssc.start()
    ssc.awaitTermination()
    //        ssc.stop()
  }

}
