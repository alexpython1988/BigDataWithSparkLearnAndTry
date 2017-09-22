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
  
  //plotting
//  val fig1 = (monthlyHighTemp.map(_._1).collect(), monthlyHighTemp.map(_._2).collect(), 0xffff0000, 4)
//  val fig2 = (monthlyLowTemp.map(_._1).collect(), monthlyLowTemp.map(_._2).collect(), 0xff00ff00, 4)
  val plot = Plot.scatterPlots(Seq(
      (monthlyHighTemp.map(_._1).collect(), monthlyHighTemp.map(_._2).collect(), 0xffff0000, 6),
      (monthlyLowTemp.map(_._1).collect(), monthlyLowTemp.map(_._2).collect(), 0xff0000ff, 6)), 
      "Temps", "month", "temp")
  FXRenderer(plot, 800, 600)
}








