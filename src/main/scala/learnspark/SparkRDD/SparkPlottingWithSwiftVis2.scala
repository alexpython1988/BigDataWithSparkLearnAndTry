package main.scala.learnspark.SparkRDD

import scalafx.application.JFXApp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import main.scala.learnspark.standardscala.TempData
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

object plotting extends JFXApp{
  val config = new SparkConf().setAppName("Plotting").setMaster("local[*]")
  val sc = new SparkContext(config)
  // change log level
  sc.setLogLevel("WARN")
  
  val lines = sc.textFile("MN212142_9392.csv")
  
  val data = lines.flatMap { line => 
    if(line.contains("Day"))
      Seq.empty
    else{
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".")
          Seq.empty
      else{
        //case class TempData(day: Int, doy: Int, month: Int, year: Int, precip: Double, snow: Double, tave: Double, tmax: Double, tmin: Double)
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, TempData.toDoubleorNeg(p(5)), TempData.toDoubleorNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
      }
    }
  }.cache()
  
  val monthlyHighTemp = data.groupBy { x => x.month }.map{
    case (m, td) =>
      m -> td.foldLeft(0.0)((sum, td) =>
        sum + td.tmax
      )/td.size
  }
  
  //monthly low temp
  val monthlyLowTemp = data.groupBy { x => x.month }.map{
    case (m, td) =>
      m -> td.foldLeft(0.0)((sum, td) =>
        sum + td.tmin
      )/td.size
  }
  
  //aggregate with key and plot
  val keyByYear = data.map { td => td.year -> td }
//use map
//  val averageYearTemp = keyByYear.aggregateByKey(0.0 -> 0)({
//    case ((sum, cnt), td) =>
//      (sum+td.tmax, cnt+1)
//  }, {
//    case((s1, c1),(s2, c2)) =>
//      (s1+s2, c1+c2)
//  }).map{ case(y, (s,c)) =>
//      (y, s/c)
//  }.collect().sortBy(_._1)
  //my way to implement: map{x => (x._1, (x._2._1/x._2._2, x._2._2))}.collect() ugly!!!
 
  //better way is to use mapValues  
  val averageYearTemp1 = keyByYear.aggregateByKey(0.0 -> 0)({
    case ((sum, cnt), td) =>
      (sum+td.tmax, cnt+1)
  }, {
    case((s1, c1),(s2, c2)) =>
      (s1+s2, c1+c2)
  }).mapValues{
    case (s,c) =>
      s/c
  }.collect().sortBy(_._1)
  
  val longTermPlot = Plot.scatterPlotWithLines(averageYearTemp1.map(_._1), averageYearTemp1.map(_._2), symbolSize=0, symbolColor=BlackARGB, lineGrouping=1)
  FXRenderer(longTermPlot, 600, 450)
  
//  val bins = (-40.0 to 110.0 by 1.0).toArray
//  val histData = data.map { x => x.tmax }.histogram(bins, true)
//  val histPlot = Plot.histogramPlot(bins, histData, 0xff00ff00, false)
//  FXRenderer(histPlot, 800, 600)
  
  //plotting
//  val fig1 = (monthlyHighTemp.map(_._1).collect(), monthlyHighTemp.map(_._2).collect(), 0xffff0000, 4)
//  val fig2 = (monthlyLowTemp.map(_._1).collect(), monthlyLowTemp.map(_._2).collect(), 0xff00ff00, 4)
  val plot = Plot.scatterPlots(Seq(
      (monthlyHighTemp.map(_._1).collect(), monthlyHighTemp.map(_._2).collect(), BlueARGB, 6),
      (monthlyLowTemp.map(_._1).collect(), monthlyLowTemp.map(_._2).collect(), YellowARGB, 6)), 
      "Temps", "month", "temp")
  FXRenderer(plot, 800, 600)
  
  //plot histgram of tmax
  val bins = (-40.0 to 110.0 by 1.0).toArray
  val histData = data.map { x => x.tmax }.histogram(bins, true)
  val histPlot = Plot.histogramPlot(bins, histData, RedARGB, false)
  FXRenderer(histPlot, 800, 600)
}








