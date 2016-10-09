/*The MIT License (MIT)

Copyright (c) 2015-2016, Joan André

Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.*/

// Import required spark classes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.{Assignment, MCL}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

/** Define main method for a start-up example*/
object Main {

  // Disable Spark messages when running program
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Guide for users who want to run MCL program
  val usage = """
    Usage: mcl [--expansionRate num] [--inflationRate num] [--epsilon num] [--maxIterations num] [--selfLoopWeight num] [--graphOrientationStrategy string]
              """

  type OptionMap = Map[Symbol, Any]

  def toInt(key: Symbol, s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => throw new Exception("\n" + key.toString() + " must be an integer")
    }
  }

  def toDouble(key: Symbol, s: String): Double = {
    try {
      s.toDouble
    } catch {
      case e: Exception => throw new Exception("\n" + key.toString() + " must be a double")
    }
  }

  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    list match {
      case Nil => map
      case "--expansionRate" :: value :: tail =>
        nextOption(map ++ Map('expansionRate -> value), tail)
      case "--inflationRate" :: value :: tail =>
        nextOption(map ++ Map('inflationRate -> value), tail)
      case "--epsilon" :: value :: tail =>
        nextOption(map ++ Map('epsilon -> value), tail)
      case "--maxIterations" :: value :: tail =>
        nextOption(map ++ Map('maxIterations -> value), tail)
      case "--selfLoopWeight" :: value :: tail =>
        nextOption(map ++ Map('selfLoopWeight -> value), tail)
      case "--graphOrientationStrategy" :: value :: tail =>
        nextOption(map ++ Map('graphOrientationStrategy -> value), tail)
      case option :: tail => throw new Exception("\nUnknown option " + option)
    }
  }

  def main(args: Array[String]) {

    // Manage options for the program
    if (args.length == 0) println(usage)
    val arglist = args.toList

    try{
      val options = nextOption(Map(),arglist)
      val expansionRate:Int = toInt('expansionRate, options.getOrElse('expansionRate, 2).toString)
      val inflationRate:Double = toDouble('inflationRate, options.getOrElse('inflationRate, 2.0).toString)
      val epsilon:Double = toDouble('epsilon, options.getOrElse('epsilon, 0.01).toString)
      val maxIterations:Int = toInt('maxIterations, options.getOrElse('maxIterations, 10).toString)
      val selfLoopWeight:Double = toDouble('selfLoopWeight, options.getOrElse('selfLoopWeight, 1.0).toString)
      val graphOrientationStrategy:String = options.getOrElse('graphOrientationStrategy, "undirected").toString

      // Initialise spark context
      val conf = new SparkConf()
        .setMaster("local[*]")
        .set("spark.driver.memory", "1g")
        .set("spark.executor.memory", "1g")
        .setAppName("MCL")

      val sc = new SparkContext(conf)

      // Create and RDD for vertices
      val users: RDD[(VertexId, String)] =
        sc.parallelize(Array((0L,"Node1"), (1L,"Node2"),
          (2L,"Node3"), (3L,"Node4"),(4L,"Node5"),
          (5L,"Node6"), (6L,"Node7"), (7L, "Node8"),
          (8L, "Node9"), (9L, "Node10"), (10L, "Node11")))

      // Create an RDD for edges
      val relationships: RDD[Edge[Double]] =
        sc.parallelize(
          Seq(Edge(0, 1, 1.0), Edge(1, 0, 1.0),
            Edge(0, 2, 1.0), Edge(2, 0, 1.0),
            Edge(0, 3, 1.0), Edge(3, 0, 1.0),
            Edge(1, 2, 1.0), Edge(2, 1, 1.0),
            Edge(1, 3, 1.0), Edge(3, 1, 1.0),
            Edge(2, 3, 1.0), Edge(3, 2, 1.0),
            Edge(4, 5, 1.0), Edge(5, 4, 1.0),
            Edge(4, 6, 1.0), Edge(6, 4, 1.0),
            Edge(4, 7, 1.0), Edge(7, 4, 1.0),
            Edge(5, 6, 1.0), Edge(6, 5, 1.0),
            Edge(5, 7, 1.0), Edge(7, 5, 1.0),
            Edge(6, 7, 1.0), Edge(7, 6, 1.0),
            Edge(3, 8, 1.0), Edge(8, 3, 1.0),
            Edge(9, 8, 1.0), Edge(8, 9, 1.0),
            Edge(9, 10, 1.0), Edge(10, 9, 1.0),
            Edge(4, 10, 1.0), Edge(10, 4, 1.0)
          ))

      // Build the initial Graph
      val graph = Graph(users, relationships)

      // Run MCL algorithm and get nodes assignments to generated clusters
      val clusters: Dataset[Assignment] =
          MCL.train(
            graph,
            expansionRate,
            inflationRate,
            epsilon,
            maxIterations,
            selfLoopWeight,
            graphOrientationStrategy)
          .assignments

      clusters
        .groupBy("cluster")
        .agg(sort_array(collect_list(col("id"))))
        .show(3)

      // Terminate spark context
      sc.stop()

    }
    catch{
      case e: Exception => println(e.getMessage)
        sys.exit(1)
    }
  }
}