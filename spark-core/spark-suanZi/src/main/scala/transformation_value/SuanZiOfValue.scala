package transformation_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/6/29 13:50
*/

object SuanZiOfValue {

  val log = LoggerFactory.getLogger(SuanZiOfValue.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("suanziOfvalue").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)

    /*
    * 1.map
    * 含义：返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
    * */
    val data: RDD[Int] = sc.parallelize(1 to 10)
    data.collect().foreach(println)
    val result: RDD[Int] = data.map(_ * 2)
    result.foreach(println)
    println("分区数：" + data.getNumPartitions)
    println("分区数：" + result.getNumPartitions)

    /*
    * 2.【FlatMap】
      含义：先执行map再执行flatten，类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
     * */
    val data1: RDD[String] = sc.makeRDD(Array("hello java", "hello scala"))
    val result1: RDD[String] = data1.flatMap(_.split(" "))
    result1.collect().foreach(println)

    /*
    * 3.【MapPartitions(func)】
    含义：类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是
    Iterator[T] => Iterator[U]
        * */
    //低效用法
    val data2: RDD[Int] = sc.parallelize(1 to 20, 2)

    def terFunc(iter: Iterator[Int]): Iterator[Int] = {
      var res = List[Int]() //定义个数组缓存数据，当数据非常大时，会有内存溢出
      while (iter.hasNext) {
        val cur = iter.next()
        res ::= (cur * 3)
      }
      return res.iterator
    }

    val value: RDD[Int] = data2.mapPartitions(terFunc)
    println(value.collect().mkString(" "))

    //高效方法
    val value3: RDD[Int] = data2.mapPartitions(x => new CustomFunc(x))
    println(value3.collect().mkString("."))
    //方法2
    val value2: RDD[Int] = data2.mapPartitions(x => {
      x.map(_ * 3)
    })
    println(value2.collect().mkString(","))

    /*
    * 4.【Glom】
        含义：glom的作用是将同一个分区里的元素合并到一个array里，返回一个RDD
    * */
    val data4: RDD[Int] = sc.parallelize(1 to 10)
    val result4: RDD[Array[Int]] = data4.glom()
    result4.foreach(x => {
      println(x.mkString(" "))
    }
    )

    /*
    * 5.【Union】
      含义：合并两个RDD，不去重，要求两个RDD中的元素类型一致
    * */
    val data5: RDD[Int] = sc.parallelize(1 to 5)
    val data51: RDD[Int] = sc.parallelize(3 to 10)
    val result5: RDD[Int] = data5.union(data51)
    println(result5.collect().mkString(" "))

    /*
    * 6.【Intersection】
        含义：对源RDD和参数RDD求交集后返回一个新的RDD
    * */
    val data6: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val data61: RDD[Int] = sc.parallelize(List(3, 4, 5))
    val result6: RDD[Int] = data6.intersection(data61)
    result6.foreach(println)
    println(result6.collect().mkString(" "))
  

  }
}

class CustomFunc(iterator: Iterator[Int]) extends Iterator[Int] {
  override def hasNext: Boolean = {
    iterator.hasNext
  }

  override def next(): Int = {
    val i: Int = iterator.next()
    i * 3
  }
}