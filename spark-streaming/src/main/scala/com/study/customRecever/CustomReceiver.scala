package com.study.customRecever

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


/*
* @author: sunxiaoxiong
* @date  : Created in 2020/3/9 10:58
*/

//string就是数据的类型
class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  override def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("Socket receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive(): Unit = {
    var socket: Socket = null
    var userInput: String = null
    try {
      //connect to host:port
      socket = new Socket(host, port)

      //Until stopped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

      userInput = reader.readLine()
      while (!isStopped() && userInput != null) {
        //内部的函数，将数据存储下来
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      // Restart in an attempt to connect again when server is active again
      restart("trying to connect again")
    } catch {
      case e: ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}

object CustomReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("networkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.receiverStream(new CustomReceiver("master01", 9999))
    val words = lines.flatMap(_.split(" "))
    words.print()
    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)
    wordCount.print()
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }
}
