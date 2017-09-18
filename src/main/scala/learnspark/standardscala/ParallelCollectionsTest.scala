package main.scala.learnspark.standardscala

object ParallelCollectionsTest extends App {
  // foldleft, reduceleft are sequetial
  val arr = Array(4,2,3,7,5,8,1,9,7,5,6,4,3,5,7,8,34,2,4,6,8,4,3,4,6,7,8,8,5,3,2,45,7,8,5,3,23)
  println(arr.foldLeft(0)(_ + _))  // => ((((((((0+4)+2)+3)+7)+5)+8)+1)+9) associated as well
  println(arr.foldRight(0)(_ + _))
  
  //when parallel
  val arr1 = Array(4,2,3,7,5,8,1,9,7,5,6,4,3,5,7,8,34,2,4,6,8,4,3,4,6,7,8,8,5,3,2,45,7,8,5,3,23).par
  println(arr1.fold(0)(_ + _)) //can obtain different results
  
  //use aggregate
  println(arr1.aggregate(0)(_ + _, _ + _)) //can obtain different results
  
}