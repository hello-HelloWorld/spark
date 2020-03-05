package com.study.LogAccumulator

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2
import scala.collection.JavaConversions._


/*
* @author: sunxiaoxiong
* @date  : Created in 2020/1/20 9:43
*/

//自定义累加器
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {

  //定义一个累加器的内存结构，用于保存带有字母的字符串
  private val _logArray: util.Set[String] = new util.HashSet[String]()

  // 重写方法检测累加器内部数据结构是否为空。
  override def isZero: Boolean = {
    //检测_logArray是否为空
    _logArray.isEmpty
  }

  //重置你的累加器的数据结构
  override def reset(): Unit = {
    //clear方法清空_logArray的所有内容
    _logArray.clear()
  }

  // 提供转换或者行动操作中添加累加器值的方法
  override def add(v: String): Unit = {
    //将带有字母的字符串添加到_logArray的内存结构中
    _logArray.add(v)
  }

  //将多个分区中的累加器的值合并的操作函数
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    // 通过类型检测将other这个累加器的值加入到当前_logArray结构中
    other match {
      case o: LogAccumulator => _logArray.addAll(other.value)
    }
  }

  //输出我的value值
  override def value: java.util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }

  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized {
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}

//过滤掉带字母的
object LogAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LogAccumulator").setMaster("local[*]")
    val spark = new SparkContext(sparkConf)

    val accum = new LogAccumulator()
    spark.register(accum, "logAccum")
    val sum = spark.parallelize(Array("1", "2a", "3e", "4f", "5", "6"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum:" + sum)
    println(accum.value)
    for (v <- accum.value) println(v + " ")
    spark.stop()
  }
}

