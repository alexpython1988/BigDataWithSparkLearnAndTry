package main.scala.learnspark.standardscala

import scalafx.application.JFXApp
import scala.io.Source
import scalafx.scene.Scene
import scalafx.scene.chart.ScatterChart
import scalafx.collections.ObservableBuffer
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.FXRenderer
import swiftvis2.plotting.ColorGradient

object PlotPractise extends JFXApp{
  val source = Source.fromFile("MN212142_9392.csv")
    val lines = source.getLines().drop(1)
    val data = lines.flatMap { line =>
      //raw data : 1,335,12,'212142',1895,0,0,12,26,-2 
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".")
        Seq.empty
      else
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, TempData.toDoubleorNeg(p(5)),  TempData.toDoubleorNeg(p(5)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
    }.toArray
    source.close()
    
    //use swiftvis (not stable)
    val cg = ColorGradient((0.0, 0xFF000000), (1.0, 0xFF00FF00), (6.0, 0xFF0000FF))
    val rainData = data.filter { x => x.precip >= 0 }.sortBy { x => x.precip }
    val xar = rainData.map { x => x.doy }
    val yar = rainData.map { x => x.tmax }
    val plot = Plot.scatterPlot(xar, yar, "Temps", "day of year", "temp", 3, rainData.map { td => cg(td.precip) })
    FXRenderer(plot, 500, 500)
    
    val cg1 = ColorGradient((0.0, 0xFF000000), (50.0, 0xFF00FF00), (60.0, 0xFF0000FF))
    val yearGrouped = data.groupBy { x => x.year }
    val yearTemp = yearGrouped.map{ case (y, td) =>
      y -> td.foldLeft(0.0)((sum, td) => sum + td.tmax)/td.length
    }
    val yearData = yearTemp.toSeq.sortBy(_._1)
    val xar1 = yearData.map(_._1.toInt)
    val yar1 = yearData.map(_._2)
    val plot1 = Plot.scatterPlot(xar1, yar1, "Temps1", "year", "tempAVG", 5, yearData.map(x => cg1(x._2)))
    FXRenderer(plot1, 500, 500)
    
//    //use scalaFX stage
//    stage = new JFXApp.PrimaryStage{
//      title = "Temp Plot"
//      scene = new Scene(500, 500){
//        val xar = NumberAxis()
//        val yar = NumberAxis()
//        val pData = XYChart.Series[Number, Number]("Temps", ObservableBuffer(data.map { x => XYChart.Data[Number, Number](x.doy, x.tmax)}:_*))
//        val plot = new ScatterChart(xar, yar, ObservableBuffer(pData))
//        root = plot
//      }
//    }
}