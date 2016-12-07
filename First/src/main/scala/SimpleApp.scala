package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
 * Created by hadoop on 2016/11/25.
 * 次程序为一个简单的spark的WordCount程序
 * 读取本地文件并统计打印出来
 *
 */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "c:/aaa.txt"
    val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile,2).persist()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    val numCs = logData.map(s => (s,1)).reduceByKey((a, b) => a + b)
//    println("Lines with a:%s, Lines with b:%s".format(numAs,numBs))
    val result = numCs.sortByKey().collect()
//    result.foreach(println)
    for(a <- result){
      println(a)
    }
//    println(result)
  }
}
