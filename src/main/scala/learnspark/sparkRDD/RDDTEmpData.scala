package main.scala.learnspark.sparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import main.scala.learnspark.standardscala.TempData

/*
 * you need to cache the data read in, otherwise every time you use the read-in data, the read in process will run
 * the way to solve this problem is to add .cache() at the end of read_in process to make read-in data reusable
 */

object RDDTEmpData {
  def main(args: Array[String]): Unit = {
    //set up spark context with proper configuration
    val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    val lines = sc.textFile("MN212142_9392.csv")
//    lines.take(5).foreach(println)
    
    val data = lines.flatMap { line =>
      //raw data : 1,335,12,'212142',1895,0,0,12,26,-2 
      if(line.contains("Day"))
        Seq.empty
      else {
        val p = line.split(",")
        if (p(7) == "." || p(8) == "." || p(9) == ".")
          Seq.empty
        else
          Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, TempData.toDoubleorNeg(p(5)), TempData.toDoubleorNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
        }
      }.cache()
    
//    println(data.count())
    
    //get max
    val maxT = data.max()(Ordering.by { x => x.tmax }).tmax
    println(maxT)
    
    val maxT1 = data.reduce((t1, t2) => if(t1.tmax > t2.tmax) t1 else t2).tmax
    println(maxT1)
      
    val rainCnt = data.filter { x => x.precip >= 1.0 }.count()
    println(s"Totally $rainCnt days have rain. Precentage of days that have rain is ${rainCnt*100/data.count}%")
    
    //average of tmax on rainy days
    val (rt, rc) = data.aggregate(0.0 -> 0)({
      case ((sum, cnt), td) =>
        if(td.precip >= 1.0) (sum+td.tmax, cnt+1) else (sum, cnt)
    }, {
      case ((rt1, rc1), (rt2, rc2)) =>
        (rt1+rt2, rc1+rc2)
    })
    println(s"average temp on rainy day is ${rt/rc}")
    
    //average of tmax on month (use groupby)
    val yearTemp = data.groupBy { x => x.year }.map({
      case (y, t) =>
        y -> t.foldLeft(0.0)({
          case (sum, t) =>
            sum+t.tmax
        })/t.size
    })
    
    yearTemp.collect().sortBy(_._1).foreach(println)
    
    //TODO perform the same job on snow
    //get Tmin: two ways
    val tMin = data.min()(Ordering.by { x => x.tmin }).tmin
    val tMin1 = data.reduce((t1, t2) => if(t1.tmin < t2.tmin) t1 else t2).tmin
    val tMin2 = data.fold(data.take(1)(0))({
      case(t1, t2) =>
        if(t1.tmin < t2.tmin) t1 else t2
    }).tmin
    println(s"The lowest temp are 1 $tMin  2 $tMin1  3 $tMin2 ")
    
    //get total number of days that has snow over 0.5 and the average tmin on these days
    //use flatmap
    val snowCnt = data.flatMap { x => 
      if(x.snow < 0.5)
        Seq.empty
      else
        Seq(x.tmin)
    }
    println(s"total is ${snowCnt.count()} and average is ${snowCnt.sum()/snowCnt.count()}")
    
    //use aggregate
    val (dc, ss) = data.aggregate(0 -> 0.0)({
      //map
      case((cnt, sum), td) =>
        if(td.snow >= 0.5) (cnt+1, sum+td.tmin) else (cnt, sum)
    }, {
      //reduce
      case ((c1, s1), (c2, s2)) =>
        (c1+c2, s1+s2)
    }) 
    println(s"total is $dc and average is ${ss/dc}")
    
    //get the average tavg for each month
    val monglyTAvg = data.groupBy(_.month).map({
      case (m, td) =>
        m -> td.foldLeft(0.0)({
          case (sum, td) =>
            sum+td.tave
        })/td.size
    })
    monglyTAvg.collect().sortBy(_._1) foreach(println)
    
    val mon = data.groupBy { x => x.month }
    mon.foreach(println) 
    //mon = ((group, CompactBuffer(data...)), (group, CompactBuffer(data...)), ...)
  }  
}







