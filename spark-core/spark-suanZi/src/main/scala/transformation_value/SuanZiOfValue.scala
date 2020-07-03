package transformation_value

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/6/29 13:50
*/

/*
* value类型的transformation算子
* 1.map   2.flatMap   3.MapPartitions(func)    4.glom   5.union   6.intersection    7.cartesian
* 8.groupBy   9.filter    10.distinct   11.subtract   12.sample   13.takeSample   14.cache,persist
* */
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
    * 含义：先执行map再执行flatten，类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
     * */
    val data1: RDD[String] = sc.makeRDD(Array("hello java", "hello scala"))
    val result1: RDD[String] = data1.flatMap(_.split(" "))
    result1.collect().foreach(println)
    println(result1.collect().mkString(",")) //hello,java,hello,scala

    /*
    * 3.【MapPartitions(func)】
    * 含义：类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是
    * Iterator[T] => Iterator[U]
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
    * 含义：glom的作用是将同一个分区里的元素合并到一个array里，返回一个RDD
    * */
    val data4: RDD[Int] = sc.parallelize(1 to 10)
    val result4: RDD[Array[Int]] = data4.glom()
    result4.foreach(x => {
      println(x.mkString(" "))
    }
    )

    /*
    * 5.【Union】
    * 含义：合并两个RDD，不去重，要求两个RDD中的元素类型一致
    * */
    val data5: RDD[Int] = sc.parallelize(1 to 5)
    val data51: RDD[Int] = sc.parallelize(3 to 10)
    val result5: RDD[Int] = data5.union(data51)
    println(result5.collect().mkString(" "))

    /*
    * 6.【Intersection】
    * 含义：对源RDD和参数RDD求交集后返回一个新的RDD
    * */
    val data6: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val partitions: Array[Partition] = data6.partitions
    for (x <- partitions) {
      println("分区数：" + x.index)
    }
    val data61: RDD[Int] = sc.parallelize(List(3, 4, 5))
    val result6: RDD[Int] = data6.intersection(data61)
    result6.foreach(println)
    println(result6.collect().mkString(" "))

    /*
    * 7.【Cartesian】
    * 含义：返回两个rdd的笛卡尔积
    * */
    val data7: RDD[Int] = sc.parallelize(List(1, 2, 3))
    val data71: RDD[String] = sc.parallelize(Array("a", "b", "c"))
    val result7: RDD[(Int, String)] = data7.cartesian(data71)
    println(result7.collect().mkString(","))

    /*
    *8.【GroupBy】
    *含义：将元素通过函数生成相应的 Key，数据就转化为 Key-Value 格式，之后将 Key 相同的元素分为一组
    *groupBy算子接收一个函数，这个函数返回的值作为key，然后通过这个key来对里面的元素进行分组
    * */
    val data8: RDD[Int] = sc.parallelize(1 to 20)
    val result8: RDD[(String, Iterable[Int])] = data8.groupBy(x => {
      if (x % 2 == 0) "yes" else "no"
    })
    println(result8.collect().mkString(" "))

    /*
    * 9.【Filter】
    *  含义：filter 函数功能是对元素进行过滤，对每个 元 素 应 用 f 函 数， 返 回 值 为 true 的 元 素 在RDD 中保留，返回值为 false 的元素将被过滤掉。
    *  内 部 实 现 相 当 于 生 成 FilteredRDD(this，sc.clean(f))
    * */
    val data9: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val result9: RDD[Int] = data9.filter(_ > 3)
    println(result9.collect().mkString(" "))

    /*
    * 10.【Distinct】
    *  含义：distinct将RDD中的元素进行去重操作
    * */
    val data10: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 3, 6, 6, 5))
    val result10: RDD[Int] = data10.distinct()
    println(result10.collect().mkString(","))

    /*
    * 11.【Subtract】
    *  含义：subtract相当于进行集合的差操作，例如RDD1去除RDD1和RDD2交集中的所有元素后再输出
    * */
    val data11: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val data11_1: RDD[Int] = sc.parallelize(List(4, 5, 6))
    val result11: RDD[Int] = data11.subtract(data11_1)
    val result11_2: RDD[Int] = data11_1.subtract(data11)

    println(result11.collect().mkString(","))
    println(result11_2.collect().mkString(",") + "=====")

    /*
    * 12.【Sample】
    *  含义：sample 将 RDD 这个集合内的元素进行采样，获取所有元素的子集。用户可以设定是否有放回的抽样、百分比、随机种子，进而决定采样方式。
    *  内部实现是生成 SampledRDD(withReplacement， fraction， seed)

    * sample算子时用来抽样用的，其有3个参数
    * withReplacement：表示抽出样本后是否在放回去，true表示会放回去，这也就意味着抽出的样本可能有重复
    * fraction ：抽出多少，这是一个double类型的参数,0-1之间，eg:0.3表示抽出30%
    * seed：表示一个种子，根据这个seed随机抽取，一般情况下只用前两个参数就可以，那么这个参数是干嘛的呢，这个参数一般用于调试，
    * 有时候不知道是程序出问题还是数据出了问题，就可以将这个参数设置为定值（如果种子一样，那么抽取到的数据是一样的）
    * */
    val data12: RDD[Int] = sc.parallelize(1 to 10)
    val result12: RDD[Int] = data12.sample(false, 0.3)
    println(result12.collect().mkString(","))

    /*
    *13.【TakeSample】
    * 含义：返回一个Array[T]，takeSample（）函数和上面的sample函数是一个原理，但是不使用相对比例采样，而是按设定的采样个数进行采样，
    * 同时返回结果不再是RDD，而是相当于对采样后的数据进行Collect()，返回结果的集合为单机的数组。注意：第二个参数是取整数
    * */
    val data13: RDD[Int] = sc.parallelize(1 to 10)
    val array: Array[Int] = data13.takeSample(false, 4, 5)
    println(array.mkString(" "))

    /*
    * 14.【Cache、Persist】
    *   含义：cache和persist都是用于将一个RDD进行缓存的，这样在之后使用的过程中就不需要重新计算了，可以大大节省程序运行时间。
    *   cache相当于 persist(MEMORY_ONLY) 函数的功能
    *
    *   两者的区别：cache只有一个默认的缓存级别MEMORY_ONLY ，而persist可以根据情况设置其它的缓存级别。
    *   persist有一个 StorageLevel 类型的参数，这个表示的是RDD的缓存级别。
    *   注意：RDD的StorageLevel只能设置一次。
    * */
    val data14: RDD[Int] = sc.parallelize(1 to 10)
    val level: StorageLevel = data14.getStorageLevel
    val data14_1: RDD[Int] = data14.cache()
    val level2: StorageLevel = data14.getStorageLevel
    val level3: StorageLevel = data14_1.getStorageLevel
    println(level)
    println(level2)
    println(level3)

    val data14_2: RDD[Int] = sc.parallelize(1 to 10)
    val data14_3: RDD[Int] = data14_2.persist(StorageLevel.MEMORY_ONLY_2)
    println(data14_3.getStorageLevel)

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