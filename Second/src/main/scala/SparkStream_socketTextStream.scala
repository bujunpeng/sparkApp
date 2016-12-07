package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by hadoop on 2016/11/25.
 * 此程序是sparkStream的一个简单例子
 * 启动本程序后，程序会监控远程机器（192.168.220.134）的端口（9999）。
 * 去远程机器上 执行命令 （nc -lk 9999）
 * 然后再输入数据，本程序就可以接受数据 并每隔10秒统计结果打印出来。
 *
 */
object SparkStream_socketTextStream {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("network")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("192.168.220.134",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()



  }
}
