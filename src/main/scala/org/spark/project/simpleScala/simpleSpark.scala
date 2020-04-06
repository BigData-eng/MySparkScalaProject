package org.spark.project.simpleScala

import org.apache.spark.sql.SparkSession

object simpleSpark extends App {



  val spark=SparkSession.builder().appName("Reading text file")
    .master(args(0))
    .getOrCreate()
  val path=args(1)
  val file =spark.read.format("csv").option("header","true").load(path)
file.printSchema()
  file.show()

}
