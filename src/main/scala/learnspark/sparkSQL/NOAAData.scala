package main.scala.learnspark.sparkSQL

import org.apache.spark.sql.SparkSession

object NOAAData {
  val sparkSession = SparkSession.builder().master("local[*]").appName("NOAAData").getOrCreate()
  import sparkSession.implicits._
  
  sparkSession.sparkContext.setLogLevel("WARN")

  /*
   * data source
   * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
   * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2017.csv.gz 
   */
  
  
  
  sparkSession.stop()
}