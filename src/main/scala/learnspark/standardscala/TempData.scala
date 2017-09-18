package main.scala.learnspark.standardscala

import scala.io.Source

case class TempData(day: Int, doy: Int, month: Int, year: Int, precip: Double, snow: Double, tave: Double, tmax: Double, tmin: Double)

object TempData {
  def toDoubleorNeg(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _: NumberFormatException => -1
    }
  }

  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("MN212142_9392.csv")

    val lines = source.getLines().drop(1)
    val data = lines.flatMap { line =>
      //raw data : 1,335,12,'212142',1895,0,0,12,26,-2 
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".")
        Seq.empty
      else
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, toDoubleorNeg(p(5)), toDoubleorNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
    }.toArray

    source.close()

    //basic information
    println(data.length)
    data.take(5).foreach(println)

    //data analysis
    // to pass
    val maxTemp = data.map { x => x.tmax }.max
    val hotDays = data.filter { x => x.tmax == maxTemp }
    println(s"Hot days are ${hotDays.mkString("; ")}")

    //one pass
    val hotDay = data.maxBy { x => x.tmax }
    println(s"The first hot day is $hotDay")

    //one pass
    val hotDay2 = data.reduceLeft((d1, d2) => if (d1.tmax >= d2.tmax) d1 else d2)
    println(s"The first hot day is $hotDay2")

    // get inch or more rain in that day
    val rainCount = data.count { x => x.precip >= 1.0 }
    println(s"The total number of days that rains over 1 inch is $rainCount")

    //The fold function perform the reduce with two operator and return one: one used as tuple for record the data we need, the other is the loop condition 
    //this function is single pass
    val (rainSum, raincount1) = data.foldLeft(0.0 -> 0) {
      case ((sum, act), td) =>
        if (td.precip < 1.0) (sum, act) else (sum + td.tmax, act + 1)
    }
    println(s"The average temp of the days that rain over 1 inch is ${rainSum / raincount1}")

    //using aggragation and parallel
    val (rs, rc) = data.par.aggregate(0.0 -> 0)({
      case ((sum, cnt), td) => //fold in parallel (must be associative)
        if (td.precip >= 1.0) (sum + td.tmax, cnt + 1) else (sum, cnt)
    }, {
      case ((r1, c1), (r2, c2)) => //reduce
        (r1 + r2, c1 + c2)
    })
     println(s"The average temp of the days that rain over 1 inch is ${rs / rc}")
    
    //perform same job use flatmap -> filter the data with condition to yield a series of collections with filtered data, then flatten all these collections into 1 collection
    val rt = data.flatMap { x =>
      if (x.precip < 1.0)
        Seq.empty
      else
        //print(Seq(x.tmax)) //List(63.0)List(74.0)List(90.0)List(83.0)List(80.0)...
        Seq(x.tmax)
    }
    //println(rt.mkString(" ")) the rt is 63.0 74.0 90.0 83.0 80.0 78.0 86.0 50.0 68.0 85.0 89.0...
    println(s"The average temp of the days that rain over 1 inch is ${rt.sum / rt.length}")

    //get ave of temp for each month -> groupby method
    val monthGroup = data.groupBy { x => x.month }
    val monthTemp = monthGroup.map {
      case (m, d) =>
        m -> d.foldLeft(0.0) { (sum, td) =>
          sum + td.tmax
        } / d.length
    }
    println(monthTemp)
    monthTemp.toSeq.sortBy(_._1).foreach(println)

    //TODO perform same task on snow
    //3 meoths to find tmin is the lowerst of the year and lowest tav as well
    val minTemp = data.map { x => x.tmin }.min
    val minTempDay = data.filter { x => x.tmin == minTemp }
    minTempDay.foreach { x => println(s"The day $x has the lowest temperature of the year") }

    val minTempDay1 = data.minBy { x => x.tmin }
    println(s"The day has the lowest temperature is $minTempDay1")

    val minTempDay2 = data.reduceLeft((td1, td2) => if (td1.tmin < td2.tmin) td1 else td2)
    println(s"The day has the lowest temperature is $minTempDay2")

    // get inch or more snow in that day, return how many days
    val snowCnt = data.count { x => x.snow >= 1.0 }
    println(s"$snowCnt days have snow more than 1 inch")

    //average max temp of the days that snow over 1 inch
    val (snowSum, snowCnt1) = data.foldLeft(0.0 -> 0) {
      case ((sum, cnt), td) =>
        if (td.snow >= 1) (sum + td.tmax, cnt + 1) else (sum, cnt)
    }
    println(s"The average temp of the days that snow over 1 inch is ${snowSum / snowCnt1}")

    //change the above method using parallel
    val (ss, sc) = data.par.aggregate(0.0 -> 0)({
      case ((sum, cnt), td) =>
        if (td.snow >= 1.0) (sum + td.tmax, cnt + 1) else (sum, cnt)
    }, {
      case ((s1, c1), (s2, c2)) =>
        (s1 + s2, c1 + c2)
    })
    println(s"The average temp of the days that snow over 1 inch is ${ss / sc}")

    //use flatmap
    val snowCnt2 = data.flatMap { x => if (x.snow >= 1.0) Seq(x.tmax) else Seq.empty }
    println(s"The average temp of the days that snow over 1 inch is ${snowCnt2.sum / snowCnt2.length}")

    //get ave of temp for each month -> groupby method
    val yearGrouped = data.groupBy { x => x.year }
    val yearTemp = yearGrouped.map {
      case (y, td) =>
        y -> td.foldLeft(0.0)((sum, td) => sum + td.tmax) / td.length
    }
    yearTemp.toSeq.sortBy(_._1).foreach(println)
  }
}











