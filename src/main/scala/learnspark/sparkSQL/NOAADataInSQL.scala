package main.scala.learnspark.sparkSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import swiftvis2.plotting.Plot
import swiftvis2.plotting.BlueARGB
import swiftvis2.plotting.GreenARGB
import swiftvis2.plotting.RedARGB
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.renderer.FXRenderer
import scalafx.application.JFXApp

/*
 * This is the same program as NOAAData but using pure spark sql instread of function
 * In general using spark sql to process table joins and select 
 * and using highlevel function to perform aggregate and other operations 
 */

object NOAADataInSQL extends JFXApp{
  //spark session set up
  val conf = new SparkConf().setAppName("NOAADataWithSQL").setMaster("local[*]")
  val sparkSession  = SparkSession.builder().config(conf).getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")
  //import sqlContext implicit conversion
  import sparkSession.sqlContext.implicits._
  
  //define schema for reading in data
  val wSchema = StructType(Array(
      StructField("sid", StringType),
      StructField("date", DateType),
      StructField("mtype", StringType),
      StructField("value", DoubleType )
  ))
  
  val sSchema = StructType(Array(
    StructField("sid", StringType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("name", StringType)
  ))
  
  //read in data with schema, use option to tell spark to format the data in certain way, in this case, we tell spark use dateFormat to formate input data with form as "yyyyMMdd"
  val data2017 = sparkSession.read.schema(wSchema).option("dateFormat", "yyyyMMdd").csv("2017.csv").cache()
  
  //if the input file is not csv, we can read in data as RDD and use Row to transfer RDD to dataframe with schema
  val stationsRDD = sparkSession.sparkContext.textFile("ghcnd-stations.txt").map { x =>  
    val sid = x.substring(0, 11)
    val lat = x.substring(12,20).toDouble
    val lon = x.substring(21,30).toDouble
    val name = x.substring(41,71)
    Row(sid, lat, lon, name)
  }
  //use createDataFrame function
  val stations = sparkSession.createDataFrame(stationsRDD, sSchema).cache()
  
  /*
   * use pure sql in spark (databricks has refs for sparksql)
   * since we need a view or table name after FROM, in spark we use "xxx.createOrReplaceTempView(yyy)" where xxx is the spark schema data and yyy is the view name used in sql
   * in the following example, the pureSql can replace the tmax we used in the line 86
   */
  data2017.createOrReplaceTempView("d2017")
  val avgTemps = sparkSession.sql("""
    select sid, date, (tmax+tmin)/20*1.8+32 as tavg from 
    (
      (select sid, date, value as tmax from d2017 where mtype='TMAX')
      join
      (select sid, date, value as tmin from d2017 where mtype='TMIN')
      using (sid, date)
    )
  """).cache()
  
  avgTemps.createOrReplaceTempView("avgT")
  //avgTemps.show()
  val stationTemp = sparkSession.sql("""
    select sid, avg(tavg) from avgT group by sid  
  """).cache() 
  
  stationTemp.createOrReplaceTempView("stationtemps")
  stations.createOrReplaceTempView("stations")
  
  val stationAvgTemp = sparkSession.sql("""
    select * from 
    (
      (select * from stationtemps) 
      join
      (select * from stations)
      using (sid)
    )  
  """).cache()
  
  //stationAvgTemp.show()
  
  //plotting
  val localData = stationAvgTemp.collect()
  val temps = localData.map { r => r.getDouble(1) }
  val lat = localData.map { r => r.getDouble(2) }
  val lon = localData.map { r => r.getDouble(3) }
  val cg = ColorGradient(0.0 -> BlueARGB, 50.0 -> GreenARGB, 100.0 -> RedARGB)
  val plot = Plot.scatterPlot(lon, lat, "Global Temps", "Longtitude", "Latitude", 4, temps.map(cg))
  FXRenderer(plot, 800, 600)
  
  sparkSession.stop()
}






