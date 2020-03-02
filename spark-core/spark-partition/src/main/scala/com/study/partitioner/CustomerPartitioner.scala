package com.study.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/2 16:39
*/

//自定义分区器改变原本的分区
class CustomerPartitioner(numParts: Int) extends Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey = key.toString

    ckey.substring(ckey.length - 1).toInt % numParts
  }
}

object CustomerPartitioner1 extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("partitioner")
  val sc = new SparkContext(conf)

  //将一个集合生成rdd
  val data = sc.parallelize(List("aa.2", "bb.3", "cc.3", "dd.4", "ee.5"))
  data.collect().foreach(println)

  val result = data.map((_, 1)).partitionBy(new CustomerPartitioner(5))
  result.mapPartitionsWithIndex((index, items) => Iterator(index + ":" + items.mkString("|"))).collect().foreach(println)
  data.mapPartitionsWithIndex((index, items) => Iterator(index + ":" + items.mkString("|"))).collect().foreach(println)


  sc.stop()
}