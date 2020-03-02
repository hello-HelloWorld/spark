package com.study.cdn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/1/20 11:28
*/

//练习
object CdnStatics {

  val logger = LoggerFactory.getLogger(CdnStatics.getClass)

  //匹配ip地址
  val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  //匹配视频文件名
  val videoPattern = "([0-9]+).mp4".r

  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
  val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配 http 响应码和请求数据大小
  val httpSizePattern = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("cdn").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val input = sc.textFile("E:\\workspace\\spark\\spark-core\\spark-cdn\\src\\main\\resources\\cdn.txt").cache()

    //统计独立IP访问量前10位
//    ipStatics(input)

    //统计每个视频独立IP数
//    videoIp(input)

    //统计一天中每个小时间的流量
    flowOfHour(input)

    sc.stop()


  }

  //1.统计一天中每个小时间的流量
  def flowOfHour(data: RDD[String]): Unit = {

    def isMatch(pattern: Regex, str: String) = {
      str match {
        case pattern(_*) => true
        case _ => false
      }
    }

    /**
      * 获取日志中小时和http 请求体大小
      *
      * @param line
      * @return
      */
    def getTimeAndSize(line: String) = {
      var res = ("", 0L)
      try {
        val httpSizePattern(code, size) = line
        val timePattern(year, hour) = line
        res = (hour, size.toLong)
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
      res
    }

    //3.统计一天中每个小时间的流量
    data.filter(x=>isMatch(httpSizePattern,x)).filter(x=>isMatch(timePattern,x)).map(x=>getTimeAndSize(x)).groupByKey()
      .map(x=>(x._1,x._2.sum)).sortByKey().foreach(x=>println(x._1+"时 CDN流量="+x._2/(1024*1024*1024)+"G"))
  }


  //2,统计每个视频独立IP数
  def videoIp(data: RDD[String]) = {
    def getFileNameAndIp(line: String) = {
      (videoPattern.findFirstIn(line).mkString, IPPattern.findFirstIn(line).mkString)
    }
    //统计每个视频独立ip个数
    data.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).map(x => getFileNameAndIp(x)).groupByKey().map(x => (x._1, x._2.toList.distinct))
      .sortBy(_._2.size, false).take(10).foreach(x => println("视频：" + x._1 + "独立个数：" + x._2.size))
  }

  //3.统计独立ip访问量前10位
  def ipStatics(data: RDD[String]): Unit = {
    //统计独立ip数

    val ipNums = data.map(x => (IPPattern.findFirstIn(x).get, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    //输出ip访问数量前10位
    ipNums.take(10).foreach(println)
    println("独立ip数：" + ipNums.count())
    logger.error("ssss")
  }
}
