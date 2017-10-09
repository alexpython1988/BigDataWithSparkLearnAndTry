package main.scala.learnspark.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.trim

case class Series(sid: String, area: String, measureCode: String, title: String)
case class LACData(id: String, year: Int, period: String, value: Double)

object BLSTypedData extends App {
  val conf = new SparkConf().setAppName("BLST").setMaster("local[*]")
  val ss = SparkSession.builder().config(conf).getOrCreate()
  import ss.implicits._
  //import ss.sqlContext.implicits._
  ss.sparkContext.setLogLevel("WARN")

  //create schema use Encoders and case class
  //use select can choose columns and apply functions from sql.functions to each column as trim or regex
  //use sample function can randomly choose certain % amount of data from the orginal dataset with or without replacement
  val countyData = ss.read.schema(Encoders.product[LACData].schema)
  .option("header", true).option("delimiter", "\t")
  .csv("la_data_64_County.txt").select(trim('id) as "id", 'year, 'period, 'value)
  .as[LACData].cache() //for production
//  .as[LACData].sample(false, 0.1).cache() //for test using sample
  
  //countyData.show()
  val seriesData = ss.read.textFile("la_series.txt").filter(!_.contains("series_id")).map { x =>
    val p = x.split("\t").map(_.trim())
    Series(p(0), p(2), p(3), p(6))
  }.cache()
  //seriesData.show()

  //join vs joinWith:  joinWith is a type-preserving join (return tuples instread of dataframe)
  val joinedOne = countyData.joinWith(seriesData, 'id === 'sid).cache()
  //joinedOne.show()
  println(joinedOne.first())

  ss.stop()
}