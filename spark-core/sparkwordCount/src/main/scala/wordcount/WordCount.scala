package wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/1/3 14:44
*/

object WordCount extends App {

  val log = LoggerFactory.getLogger(WordCount.getClass)

  //声明配置
  val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]") //使用本地模式运行，没有使用集群

  //创建sparkContext，用于连接spark
  val sc = new SparkContext(sparkConf)

  //业务逻辑
  val file = sc.textFile("hdfs://master1:9000//xiong_test2/student/student.txt")
  val words = file.flatMap(_.split(" "))
  val wordCount = words.map((_, 1))
  val result = wordCount.reduceByKey(_ + _)
  result.saveAsTextFile("hdfs://master1:9000/xiong_test2/student/abc")

  //关闭spark连接
  sc.stop()
}
