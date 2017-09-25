package main.scala.learnspark.SparkRDD

import scalafx.application.JFXApp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//case class == enum
case class Area(code: String, text: String)
case class Series(id: String, area: String, measureCode: String, title: String)
case class LAData(id: String, year: Int, period: Int, value: Double)

object RDDUnemployment extends JFXApp{
  val conf = new SparkConf().setAppName("Unemployment").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val areaData = sc.textFile("la_area.txt")
                //remove the firstline
                .filter { x => !x.contains("area_type_code") }
                //create a RDD area data
                .map { x => 
                  val each = x.split("\t").map(_.trim())
                  Area(each(1), each(2)) 
                }.cache()
  areaData.take(5) foreach println
  
  val seriesData = sc.textFile("la_series.txt").flatMap { x =>
    if(x.contains("area_type"))
      Seq.empty
    else{
      val each = x.split("\t").map(_.trim())
      Seq(Series(each(0), each(2), each(3), each(6)))
    }
  }.cache()
  seriesData.take(5) foreach println
                
  val MNData = sc.textFile("la_data_30_Minnesota.txt").flatMap { line =>
    if(line.contains("year"))
      Seq.empty
    else{
      val each = line.split("\t").map(_.trim())
      Seq(LAData(each(0), each(1).toInt, each(2).filter(_.isDigit).toInt, each(3).toDouble))
    }
  }.cache()
  MNData.take(5) foreach println
  
  //TASK: 1990s vs other decades on average of unemploy rates
  // id with end == 03 is the unemployed rate which is the type of data we are interested
  val rates = MNData.filter { x => x.id.endsWith("03") }
  //group the data based on id and decades(year/10)
  val decadeGroups = rates.map { d =>
    (d.id, d.year/10) -> d.value
  }
  
  //val myDecadeAvg = decadeGroups.reduceByKey(_ + _)  
  //calc sum of value and key appeared total times based on each key from map
  val decadeSum = decadeGroups.aggregateByKey(0.0 -> 0)({
    case((s, c), d) =>
      (s+d, c+1)
  }, {
    case((s1, c1), (s2, c2)) =>
      (s1+s2, c1+c2)
  }).sortBy(_._1._2)
  
  //get the average
  val decadeAvg = decadeSum.mapValues{case(s,c) => s/c}.sortBy(_._1._2)
  
  decadeSum.take(5) foreach println
  decadeAvg.take(5) foreach println
  println("*"*20)
  
  //flat the key to only id not include the decades example: ((id,decade), avg) => (id, (decade, avg))
  val maxDecadeAvg = decadeAvg.map{
    case((id, decade), avg) =>
      id -> (decade*10+"s", avg)
  }.reduceByKey{
    case((d1, a1), (d2, a2)) =>
      if(a1 >= a2) (d1, a1) else (d2, a2)
  }
  
  maxDecadeAvg.take(5) foreach println
  
  //join maxDecadeAvg(RDD) with series to find id related area name
  val seriesPairs = seriesData.map { x => x.id -> x.title}
  val seriesMaxDecadeAvg = seriesPairs.join(maxDecadeAvg)
  seriesMaxDecadeAvg.take(5) foreach println
  
  //join maxDecadeAvg with area and series to get only city name, dacades and avg
  seriesData.map { x => x.id -> (x.area, x.title) }.join(maxDecadeAvg).map{
    case(sid,((aid, t), (d, a))) =>
      aid -> (t,(d, a))
  }.join(areaData.map { x => x.code -> x.text })
  .mapValues{
    case((t,(d,a)), loc) =>
      (t.split(":")(1) , loc, d, a)
  }
////if need to remove the key, use the code below  
//  .map{
//    case(id,((t,(d,a)), loc)) =>
//      (loc, t.split(":")(1)) -> (d, a)
//  }
  .take(10).foreach(println)
  
  //plotting
  
  sc.stop()
}











