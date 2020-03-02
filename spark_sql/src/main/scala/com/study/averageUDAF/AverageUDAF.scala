package com.study.averageUDAF

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/1/17 15:50
*/

/*{"name":"Michael", "salary":3000}
{"name":"Andy", "salary":4500}
{"name":"Justin", "salary":3500}
{"name":"Berta", "salary":4000}
  目标：求平均工资  【工资的总额，工资的个数】
*/
//弱类型用户自定义聚合函数
class AverageUDAF extends UserDefinedAggregateFunction {
  //输入数据的数据类型
  override def inputSchema: StructType = StructType(StructField("salary2", LongType) :: Nil)

  //每一个分区中的共享变量
  override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", IntegerType) :: Nil)

  //表示UDAF的输出类型
  override def dataType: DataType = DoubleType

  //表示相同的输入是否会存在相同的输出，如果是则设置为true
  override def deterministic: Boolean = true

  //初始化每一个分区中的共享变量
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0
  }

  //每一个分区中的每一条数据聚合的时候都需要调用此方法,相同Execute间的数据合并。
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //获取这一行中的工资，然后将工资加到sum中
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    //将工资的个数加1
    buffer(1) = buffer.getInt(1) + 1
  }

  //将每一个分区的输出进行合并，形成最后的数据;不同Execute间的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //合并总的工资
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //合并总的工资个数
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    //取出总的工资/总的工资个数
    buffer.getLong(0).toDouble / buffer.getInt(1)
  }
}

//伴生类对象
object AverageUDAF extends App {
  //获取sparkconf
  val sparkConf = new SparkConf().setAppName("udaf").setMaster("local[*]")
  //获取spark
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  //读取数据
  val employee = spark.read.json("E:\\workspace\\spark\\spark_sql\\src\\main\\resources\\employees.json")
  //创建临时表
  employee.createOrReplaceTempView("employee")
  //注册udf函数
  spark.udf.register("average", new AverageUDAF)

  spark.sql("select average(salary) from employee").show()
  spark.stop()
}
