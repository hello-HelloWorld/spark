package com.study.hive

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/2 17:44
*/
case class Record(key: Int, value: String)

object HiveOperation {
  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("spark hive demo")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("create table if not exists aa(key INT,value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' into table aa")

    val df = spark.sql("select * from aa")
    df.show()

    df.write.format("json").save("hdfs://master01:9000/ss.json")
    spark.stop()
  }

}
