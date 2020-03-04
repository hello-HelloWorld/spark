package com.study.unsafeMyAverage

import org.apache.calcite.avatica.ColumnMetaData
import org.apache.derby.impl.sql.execute.UserDefinedAggregator
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/4 11:03
*/

object MyAverage extends UserDefinedAggregateFunction {

  //聚合函数输入参数的数据类型
  override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  //聚合缓冲区中的数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  //返回值数据类型
  override def dataType: DataType = DoubleType

  //对于相同的输入是否一直返回相同的输出
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //存工资的总额
    buffer(0) = 0L
    //存工资的个数
    buffer(1) = 0L
  }

  //相同execute间的数据合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  //不同execute间的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)

  def main(args: Array[String]): Unit = {
    //创建sparkConf并配置
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spa")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    //注册udf函数
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("E:\\workspace\\spark\\spark_sql\\src\\main\\resources\\employees.json")
    df.createOrReplaceTempView("employee")
    df.show()

    val result = spark.sql("select myAverage(salary) as average_salary from employee")
    result.show()

    spark.stop()
  }
}
