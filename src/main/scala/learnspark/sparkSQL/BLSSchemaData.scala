package learnspark.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BLSSchemaData extends App{
  val conf = new SparkConf().setAppName("BLS").setMaster("local[*]")
  val ss = SparkSession.builder().config(conf).getOrCreate()
  ss.sparkContext.setLogLevel("WARN")
  import ss.implicits._
  
  
}