package com.study.sparksql

import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/1/16 11:24
*/

object HelloWord {

  val logger: Logger = LoggerFactory.getLogger(HelloWord.getClass)

  def main(args: Array[String]): Unit = {
    //创建sparkConf()并设置app名称
    val spark = SparkSession
      .builder() //创建SparkSession
      .appName("sparksql")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport() //需要hive支持
      .getOrCreate()

    //需要导入这个包
    //通过引入隐式转换可以将RDD的操作添加到DataFrame上
    import spark.implicits._

    //创建得到DataFrames
    val df = spark.read.json("file:///opt/spark/examples/src/main/resources/people.json")
    //show操作类似于Action，将DataFrame直接打印到Console。
    df.show()
    // DSL风格的使用方式中，属性的获取方法
    df.filter($"age" > 21).show()
    // 将DataFrame注册为一张临时表
    df.createOrReplaceTempView("persons")
    // 通过spark.sql方法来运行正常的SQL语句。
    spark.sql("select * from persons where age>21").show()
    // 关闭整个SparkSession。
    spark.stop()
  }
}
