package com.study.partice

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/3 15:38
*/

case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

object Practice {

  //将DataFrame插入到hive表
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("drop table if exists" + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  private def insertMysql(tableName: String, dataDF: DataFrame): Unit = {
    dataDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparksql")
      .option("tableName", tableName)
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def main(args: Array[String]): Unit = {
    //创建spark配置
    val conf = new SparkConf().setAppName("practice").setMaster("local[*]")
    //创建sparksql客户端
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    //加载数据到hive
    val tbStockRDD = spark.sparkContext.textFile("E:\\workspace\\spark\\spark_sql\\src\\main\\resources\\tbStock.txt")
    val tbStockDS = tbStockRDD.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS()
    insertHive(spark, "tbStock", tbStockDS.toDF())

    val tbStockDetailRdd = spark.sparkContext.textFile("E:\\workspace\\spark\\spark_sql\\src\\main\\resources\\tbStockDetail.txt")
    val tbStockDetailDs = tbStockDetailRdd.map(_.split(",")).map(attr => tbStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble))
    insertHive(spark, "tbStockDetail", tbStockDetailDs.toDF())

    val tbStockDateRdd = spark.sparkContext.textFile("E:\\workspace\\spark\\spark_sql\\src\\main\\resources\\tbDate.txt")
    val tbStockDateDs = tbStockDateRdd.map(_.split(",")).map(attr => tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim.toInt, attr(5).trim.toInt, attr(6).trim.toInt, attr(7).trim.toInt, attr(8).trim.toInt, attr(9).trim.toInt))
    insertHive(spark, "tbStockDate", tbStockDateDs.toDF())

    //需求1，统计所有订单中每年的销售单数，销售总额
    val result = spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear")
    insertMysql("xq1", result)

    //需求2，统计每年最大金额订单的销售额
    val result2 = spark.sql("SELECT theyear, MAX(c.SumOfAmount) AS SumOfAmount FROM (SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber GROUP BY a.dateid, a.ordernumber ) c JOIN tbDate d ON c.dateid = d.dateid GROUP BY theyear ORDER BY theyear DESC")
    insertMysql("xq2", result2)

    //需求3，统计每年最畅销产品
    val result3 = spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) e JOIN (SELECT d.theyear, MAX(d.sumofamount) AS maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear ) f ON e.theyear = f.theyear AND e.sumofamount = f.maxofamount ORDER BY e.theyear")
    insertMysql("xq3", result3)

    spark.close()
  }
}
