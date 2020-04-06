package org.spark.project.simpleScala

case class line (name: String,age: String,city: String)
object SimpleScala extends App {

  println("This is Simple Class")

  val s= Seq(("Govind","14","Govardhan"),("Krsna","16","Vrindavan"))

  println(s.contains("Govind"))

  val data = s.map(s => line(s._1,s._2,s._3)) // Data is list of line

  println("data is " + data(0))
  println("Name is " + data(0).name)

  println("data is " + data(1))
  println("Age is " + data(1).age)

}
