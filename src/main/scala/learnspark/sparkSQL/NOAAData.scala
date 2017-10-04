package main.scala.learnspark.sparkSQL

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

object NOAAData extends JFXApp{
  //set up spark sql evn
  val sparkSession = SparkSession.builder().master("local[*]").appName("NOAAData").getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")
  //must import sqlContext.implicits under current session name to facilitate all the implicit conversion
  import sparkSession.sqlContext.implicits._
  
  /*
   * data source
   * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
   * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2017.csv.gz 
   */
  
  //define schema
  val weatherSchema = StructType(Array(
    StructField("sid", StringType),
    StructField("date", DateType),
    StructField("mtype", StringType),
    StructField("value", DoubleType)
  ))
  
  //use read.schema to apply the schema to the data and use option to perform format 
  val data2017 = sparkSession.read.schema(weatherSchema).option("dateFormat", "yyyyMMdd").csv("2017.csv").cache() 
  //data2017.show()
  //show the schema structure
  //data2017.schema.printTreeString()
  
  //read in ghcnd-stations.txt file, since it is not a csv, we have read in as RDD then convert to dataframe
  val stationSchema = StructType(Array(
    StructField("sid", StringType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("name", StringType)
  ))
  
  val sRDD = sparkSession.sparkContext.textFile("ghcnd-stations.txt").map { row =>
    val id = row.substring(0,11)
    val lat = row.substring(12,20).toDouble
    val lon = row.substring(21,30).toDouble
    val name = row.substring(41,71)
    //make the read-in data as RDD[Row]
    Row(id, lat, lon, name)
  }
  
  //convert RDD[Row] to dataframe
  val stations = sparkSession.createDataFrame(sRDD, stationSchema).cache()
  //stations.show()
  
  /*
   * the filter function for dataframe take column as param
   * the column param can be obtain in several ways (find in doc)
   * the most two common ways are using $columnname and 'columnname()
   * 'xxx -> scala symbol
   */
//  val tmax = data2017.filter(data2017("mtype") === "TMAX") // using apply function
  //if run on clusters, we do not need limit, using limit because of memory size issue
  val tmax = data2017.filter($"mtype" === "TMAX").drop("mtype").withColumnRenamed("value", "tmax").cache()
  //tmax.show()
  
  val tmin = data2017.filter('mtype === "TMIN").drop("mtype").withColumnRenamed("value", "tmin").cache()
  //tmin.show()
  
  //get the tmax tmin for same sid and same day
  //use seq(xx, yy) only produce xx yy in the new table once (xx and yy are the keys to join on) 
  val combinedTemps = tmax.join(tmin, Seq("sid", "date")).cache()
  //combinedTemps.show()
  
  //get average using select
  val avgTemp = combinedTemps.select('sid, 'date, 'tmax, 'tmin, (('tmax + 'tmin)/20*1.8+32)).withColumnRenamed("((((tmax + tmin) / 20) * 1.8) + 32)", "tavg").cache()
  //avgTemp.show()
  //get statistics of the dataframe
  //avgTemp.describe().show()
  
  //combine avgTemp with stations and make the avgTemp which is daily averge to station average (averaged based on stations for all time)
  //get temp group by station id and aggregate the value by using avg method
  val stationTemp = avgTemp.groupBy('sid).agg(avg('tavg)).withColumnRenamed("avg(tavg)", "tavg")
  //stationTemp.show()
  //join
  val stationAvgTemp = stations.join(stationTemp, Seq("sid")).cache()
  //stationAvgTemp.show()
  
  //plotting
  val localData = stationAvgTemp.collect()
  val temps = localData.map { r => r.getDouble(4) }
  val lat = localData.map { r => r.getDouble(1) }
  val lon = localData.map { r => r.getDouble(2) }
  val cg = ColorGradient(0.0 -> BlueARGB, 50.0 -> GreenARGB, 100.0 -> RedARGB)
  val plot = Plot.scatterPlot(lon, lat, "Global Temps", "Longtitude", "Latitude", 4, temps.map(cg))
  FXRenderer(plot, 800, 600)
  
  sparkSession.stop()
}







