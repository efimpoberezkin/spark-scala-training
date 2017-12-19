package com.epam.homework

import org.apache.spark.{SparkConf, SparkContext}

object App {

  val verticesFile = "src/main/resources/vertices.txt"
  val edgesFile = "src/main/resources/edges.txt"
  val iterations = 10
  val topAmount = 10

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Reading (link id, page name) pairs
    val vertices = sc.textFile(verticesFile)
    val pageNames = vertices.map(line => (line.split("\t")(0), line.split("\t")(1)))

    // Reading (link id, link id) pairs
    val edges = sc.textFile(edgesFile)
    val linksPairs = edges.map(line => (line.split("\t")(0), line.split("\t")(1))).persist()
    val outgoingLinks = linksPairs.groupByKey()

    // Amount of outgoing links for each page
    val numNeighbors = outgoingLinks.map(tup => (tup._1, tup._2.size)).persist()

    // Initial ranks equal to 1
    var ranks = outgoingLinks.map(tup => (tup._1, 1.0))

    // Iteratively calculating contributions and updating page ranks
    for (i <- 1 to iterations) {
            val outgoingContributions = ranks.join(numNeighbors).map(tup => (tup._1, tup._2._1 / tup._2._2))
            val incomingContributions = linksPairs.join(outgoingContributions).values.reduceByKey(_ + _)
            ranks = incomingContributions.map(tup => (tup._1, 0.15 + 0.85 * tup._2))
    }

    // Printing top ranked pages
    val topLinks = pageNames.join(ranks).values.sortBy(_._2, ascending = false).take(topAmount)
    topLinks.foreach(tup => println(tup._1 + " --- rank: " + tup._2))
  }
}