package com.epam.homework

import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]) {
    val conf = new SparkConf() setAppName "PageRank" setMaster "local[2]"
    val sc = new SparkContext(conf)

    println("Hello World!")
  }
}