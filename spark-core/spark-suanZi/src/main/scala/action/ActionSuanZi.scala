package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import transformation_value.SuanZiOfValue

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/7/2 16:01
*/

object ActionSuanZi {

  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("action").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)

    /*
    * 1.【Foreach】
    * 含义：foreach是对RDD中的每一个元素执行指定的函数
    * */
    val rdd1: RDD[String] = sc.parallelize(List("dog", "cat", "pi"))
    rdd1.foreach(x => {
      println(x.length)
    })

    /*
    *2.foreachPartition
    * 它跟foreach的功能差不多，但是比foreach的效率高
    * */
    rdd1.foreachPartition(rdd => {
      rdd.foreach(x => {
        println(x.length)
      })
    })

    /*
    * 3.【SaveAsTextFile】
    *  含义：将此RDD保存为一个压缩文本文件，使用元素的字符串表示形式。
    *  函数原型：
    *  def saveAsTextFile(path: String): Unit
    *  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit
    *  第一个参数：Path为保存的路径；
    *  第二个参数：codec为压缩编码格式；
    *  saveAsTextFile 函数的内部实现，其内部是通过调用 saveAsHadoopFile 进行实现：
    *  this.map(x => (NullWritable.get(), new Text(x.toString))).saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
    *  将 RDD 中的每个元素映射转变为 (null， x.toString)，然后再将其写入 HDFS。
    * */

    val rdd3: RDD[String] = sc.makeRDD(List("dg", "aa", "bb", "cc", "dd"))
    //    rdd3.saveAsTextFile("D:\\asd")

    /*
    * 4.【Collect】
    * 含义：以数据的形式返回数据集中的所有元素给Driver程序，为防止Driver程序内存溢出，一般要控制返回的数据集大小
    * 扩充：
    * 1.collect的作用
    * Spark内有collect方法，是Action操作里边的一个算子，这个方法可以将RDD类型的数据转化为数组，同时会从远程集群是拉取数据到driver端。
    * 2.已知的弊端
    * 首先，collect是Action里边的，根据RDD的惰性机制，真正的计算发生在RDD的Action操作。那么，一次collect就会导致一次Shuffle，而一次Shuffle调度一次stage，然而一次stage包含很多个已分解的任务碎片Task。这么一来，会导致程序运行时间大大增加，属于比较耗时的操作，即使是在local模式下也同样耗时。
    * 其次，从环境上来讲，本机local模式下运行并无太大区别，可若放在分布式环境下运行，一次collect操作会将分布式各个节点上的数据汇聚到一个driver节点上，而这么一来，后续所执行的运算和操作就会脱离这个分布式环境而相当于单机环境下运行，这也与Spark的分布式理念不合。
    * 最后，将大量数据汇集到一个driver节点上，并且像这样val arr = data.collect()，将数据用数组存放，占用了jvm堆内存，可想而知，是有多么轻松就会内存溢出。
    * 3.如何规避
    * 若需要遍历RDD中元素，大可不必使用collect，可以使用foreach语句；
    * 若需要打印RDD中元素，可用take语句，返回数据集前n个元素，data.take(1000).foreach(println)，这点官方文档里有说明；
    * 若需要查看其中内容，可用saveAsTextFile方法。
    * 总之，单机环境下使用collect问题并不大，但分布式环境下尽量规避，如有其他需要，手动编写代码实现相应功能就好。
    * 4.补充：
    * collectPartitions：同样属于Action的一种操作，同样也会将数据汇集到Driver节点上，与collect区别并不是很大，唯一的区别是：collectPartitions产生数据类型不同于collect，collect是将所有RDD汇集到一个数组里，而collectPartitions是将各个分区内所有元素存储到一个数组里，再将这些数组汇集到driver端产生一个数组；collect产生一维数组，而collectPartitions产生二维数组。
    * 函数原型：
    * def collect(): Array[T]
    * def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U]
    * collect函数的定义有两种，我们最常用的是第一个。第二个函数需要我们提供一个标准的偏函数，然后保存符合的元素到MappedRDD中。
    * */

    /*
    * 5.【CollectAsMap】
    * 含义：功能和collect函数类似，作用于K-V类型的RDD上，作用与collect不同的是collectAsMap函数不包含重复的key，对于重复的key。后面的元素覆盖前面的元素
    * 函数原型：
    * def collectAsMap(): Map[K, V]
    * 注意：map中如果有相同的key，其value只保存最后一个值
    * */
    val rdd5: RDD[(String, Int)] = sc.parallelize(List(("aa", 1), ("bb", 1), ("cc", 3), ("aa", 2), ("cc", 1)))
    val result5: collection.Map[String, Int] = rdd5.collectAsMap()
    println(result5.mkString(","))

    /*
    * 6.【ReduceByKeyLocally】
    * 含义：实现的是先reduce再collectAsMap的功能，先对RDD的整体进行reduce操作，然后再收集所有结果返回为一个HashMap
    *函数原型：
    *def reduceByKeyLocally(func: (V, V) => V): Map[K, V]
    *该函数将RDD[K,V]中每个K对应的V值根据映射函数来运算，运算结果映射到一个Map[K,V]中，而不是RDD[K,V]。
    * */
    val result6: collection.Map[String, Int] = rdd5.reduceByKeyLocally((x, y) => {
      x + y
    })
    println(result6.mkString(","))

    /*
    *
    * */
  }
}
