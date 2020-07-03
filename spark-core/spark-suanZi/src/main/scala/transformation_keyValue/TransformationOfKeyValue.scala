package transformation_keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/6/30 13:28
*/
/*
* key-value类型的transformation算子
* 1.mapValues   2.CombineByKey    3.reduceByKey   4.partitionBy   5.cogroup   6.join    7.leftouterJoin,rightOuterJoin
* */
object TransformationOfKeyValue {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transformation-keyValue")
    val sc: SparkContext = new SparkContext(sparkConf)

    /*
   * 1.【MapValues】
   *  含义：针对（Key， Value）型数据中的 Value 进行 Map 操作，而不对 Key 进行处理
   * */
    val data1: RDD[String] = sc.parallelize(List("cat", "tiger", "monkey", "dog", "pig"))
    val data1_1: RDD[(Int, String)] = data1.map(x => {
      (x.length, x)
    })
    val tuples: Array[(Int, String)] = data1_1.mapValues(_ + " is animal").collect()
    println(tuples.mkString(","))

    /*
    * 2.【CombineByKey】
    *  含义：和reduceByKey是相同的效果，
    *  第一个参数x:原封不动取出来,
    *  第二个参数:是函数, 局部运算,
    *  第三个:是函数, 对局部运算后的结果再做运算，combineByKey是spark中更加底层，更加难理解，但却更灵活的一个算子
    * */
    val data2: RDD[(String, Int)] = sc.parallelize(List(("cat", 2), ("dog", 3), ("pig", 4), ("tiger", 5), ("monkey", 6), ("cat", 4)), 4)
    val result1: RDD[(String, Int)] = data2.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
    println(result1.getNumPartitions)
    println(result1.collect().mkString(","))

    /*
    * 3.【ReduceByKey】
    * 含义：reduceByKey的作用对像是(key, value)形式的rdd，而reduce有减少、压缩之意，reduceByKey的作用就是对相同key的数据进行处理，
    * 最终每个key只保留一条记录。保留一条记录通常有两种结果。一种是只保留我们希望的信息，比如每个key出现的次数。
    * 第二种是把value聚合在一起形成列表，这样后续可以对value做进一步的操作，比如排序。
    *
    * reduceByKey和groupByKey区别：
    * reduceByKey：reduceByKey会在结果发送至reducer之前会对每个mapper在本地进行merge，有点类似于在MapReduce中的combiner。
    * 这样做的好处在于，在map端进行一次reduce之后，数据量会大幅度减小，从而减小传输，保证reduce端能够更快的进行结果计算。
    * groupByKey：groupByKey会对每一个RDD中的value值进行聚合形成一个序列(Iterator)，此操作发生在reduce端，
    * 所以势必会将所有的数据通过网络进行传输，造成不必要的浪费。同时如果数据量十分大，可能还会造成OutOfMemoryError。
    *
    * 通过以上对比可以发现在进行大量数据的reduce操作时候建议使用reduceByKey。不仅可以提高速度，还是可以防止使用groupByKey造成的内存溢出问题
    * */
    val data3: RDD[(String, Int)] = sc.parallelize(List(("dog", 1), ("cat", 1), ("dog", 2), ("cat", 2), ("pig", 1)))
    val result3: RDD[(String, Int)] = data3.reduceByKey(_ + _)
    println(result3.collect().mkString)

    /*
    * 4.【PartitionBy】
    * 含义：partitionBy函数对RDD重新进行分区操作。
    * 函数原型：
    * def partitionBy(partitioner: Partitioner): RDD[(K, V)]
    * 该函数根据partitioner函数生成新的ShuffleRDD，将原RDD重新分区。
    * */
    val data4: RDD[(String, Int)] = sc.parallelize(List(("dog", 1), ("cat", 1), ("pig", 1), ("monkey", 1), ("aa", 1), ("bb", 1), ("cc", 1)), 2)
    val array: Array[Array[(String, Int)]] = data4.glom().collect()
    for (x <- array) {
      println(x.mkString(","))
    }

    val data4_1: RDD[(String, Int)] = data4.partitionBy(new HashPartitioner(4))
    println(data4_1.getNumPartitions)
    val array2: Array[Array[(String, Int)]] = data4_1.glom().collect()
    for (x <- array2) {
      println(x.mkString(","))
    }

    /*
    * 5.【Cogroup】
    *  含义：cogroup与join算子不同的是如果rdd中的一个key,对应多个value,则返回<Iterable<key>,Iterable<value>>
    *  在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
    * */
    val data5: RDD[Int] = sc.parallelize(List(1, 2, 3, 1, 1))
    val data5_1: RDD[(Int, String)] = sc.parallelize(data5.map((_, "a")).collect())
    val data5_2: RDD[(Int, String)] = sc.parallelize(data5.map((_, "b")).collect())
    val data5_3: RDD[(Int, String)] = sc.parallelize(List((1, "a"), (1, "b"), (1, "c"), (2, "c")))
    val result5: RDD[(Int, (Iterable[String], Iterable[String]))] = data5_1.cogroup(data5_2)
    val result5_1: RDD[(Int, (Iterable[String], Iterable[String]))] = data5_3.cogroup(data5_2)
    println(result5.collect().mkString(",")) //(1,(CompactBuffer(a, a, a),CompactBuffer(b, b, b))),(2,(CompactBuffer(a),CompactBuffer(b))),(3,(CompactBuffer(a),CompactBuffer(b)))
    println(result5_1.collect().mkString("_")) //(1,(CompactBuffer(a, b, c),CompactBuffer(b, b, b)))_(2,(CompactBuffer(c),CompactBuffer(b)))_(3,(CompactBuffer(),CompactBuffer(b)))

    /*
    * 6.【Join】
    * 含义：在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
    * */
    val result6: RDD[(Int, (String, String))] = data5_1.join(data5_2)
    println(result6.collect().mkString("_"))

    /*
    *7.【LeftOuterJoin、RightOuterJoin】
    * 含义：LeftOutJoin（左外连接）和RightOutJoin（右外连接）相当于在join的基础上先判断一侧的RDD元素是否为空，如果为空，则填充为空。
    * 如果不为空，则将数据进行连接运算，并返回结果。
    * */
    val data7: RDD[(String, Any)] = sc.parallelize(List(("dog", 1), ("cat", "1"), ("pig", 1)))
    val data7_1: RDD[(String, Int)] = sc.parallelize(List(("cat", 2), ("pig", 2), ("monkey", 2)))
    val result7: RDD[(String, (Any, Option[Int]))] = data7.leftOuterJoin(data7_1)
    val result7_1: RDD[(String, (Option[Any], Int))] = data7.rightOuterJoin(data7_1)
    println(result7.collect().mkString("_"))
    println(result7_1.collect().mkString("_"))
  }
}
