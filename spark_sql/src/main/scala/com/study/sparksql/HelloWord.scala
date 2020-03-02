package com.study.sparksql

import org.apache.spark.sql.SparkSession
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
    import spark.implicits._
    //创建得到DataFrames
    val df = spark.read.json("file:///opt/spark/examples/src/main/resources/people.json")
    df.show()
    df.filter($"age" > 21).show()
    df.createOrReplaceTempView("persons")
    spark.sql("select * from persons where age>21").show()
    spark.stop()
  }
}
