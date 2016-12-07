package main.scala

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 2016/12/7.
 *
 * 次程序为监控本地一个目录，如果目录中导入文件，则解析文件，输出WordCount。
 * 未完成
 *
 */
object SparkStream_fileStream {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("fileStream")
    val ssc = new StreamingContext(conf,Seconds(10))
    val lines = ssc.textFileStream("C:\\test\\")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
