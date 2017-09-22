package main.scala.jiuzhangbigdata

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    val text = sc.textFile("HarryPoter.txt", 4)
    //text.take(100).foreach { x => println(x) }
    val counts = text.flatMap { each => each.toLowerCase().split("\\W+")}.map{
      word => (word, 1)
    }.reduceByKey(_ + _)
    
    counts.filter(_._1 != "")
    
    counts.collect().sortBy(- _._2).take(10) foreach(println)
  }
}