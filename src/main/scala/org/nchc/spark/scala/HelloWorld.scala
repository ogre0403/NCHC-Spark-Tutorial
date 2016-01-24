package org.nchc.spark.scala

import org.apache.spark._

object HelloWorld {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("HelloWorld")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val lines =  sc.textFile(inputFile)
    val pythonLines = lines.filter(line => line.contains("Python"))
    val result = pythonLines.collect()

    for(e <- result) println(e)

  }
}