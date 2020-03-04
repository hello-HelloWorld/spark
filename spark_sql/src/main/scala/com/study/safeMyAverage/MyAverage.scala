package com.study.safeMyAverage

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/3 17:01
*/

// 既然是强类型，可能有case类
case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

class MyAverage extends Aggregator[Employee, Average, Double] {

  //定义一个数据结构，保存工资总数和工资总个数，初始都为0
  override def zero: Average = Average(0L, 0L)

  //聚合相同executor分片中的结果
  override def reduce(buffer: Average, employee: Employee): Average = {
    //工资总数
    buffer.sum += employee.salary
    //工资个数
    buffer.count += 1
    buffer
  }

  //聚合不同execute的结果
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //计算输出
  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  // 设定中间值类型的编码器，要转换成case类
  // Encoders.product是进行scala元组和case类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  //设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object MyAverage {
  def main(args: Array[String]): Unit = {
    //创建sparkconf并配置
    val spark = SparkSession
      .builder()
      .appName("spark sql basic example")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.json("E:\\workspace\\spark\\spark_sql\\src\\main\\resources\\employees.json").as[Employee]
    ds.show()

    val averageSalary = new MyAverage().toColumn.name("average_salary")

    val result = ds.select(averageSalary)
    result.show()

    spark.stop()
  }
}
