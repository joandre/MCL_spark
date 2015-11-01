/*The MIT License (MIT)

Copyright (c) 2015, Joan André

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

import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.{Assignment, MCL}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

// Define main method (scala entry point)
object Main {

  // Disable Spark messages when running programm
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Guide for users who want to run MCL programm
  val usage = """
    Usage: mcl [--expansionRate num] [--inflationRate num] [--convergenceRate num] [--epsilon num] [--maxIterations num]
              """

  def main(args: Array[String]) {

    // Manage options for the programm
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--expansionRate" :: value :: tail =>
          nextOption(map ++ Map('expansionRate -> value.toDouble), tail)
        case "--inflationRate" :: value :: tail =>
          nextOption(map ++ Map('inflationRate -> value.toDouble), tail)
        case "--convergenceRate" :: value :: tail =>
          nextOption(map ++ Map('convergenceRate -> value.toDouble), tail)
        case "--epsilon" :: value :: tail =>
          nextOption(map ++ Map('epsilon -> value.toDouble), tail)
        case "--maxIterations" :: value :: tail =>
          nextOption(map ++ Map('maxIterations -> value.toInt), tail)
        /*case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)*/
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)

    // Initialise spark context
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MCL")
      .set("spark.executor.memory","1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction","1")

    val sc = new SparkContext(conf)

    // Create and RDD for vertices
    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array((0L,"Node1"), (1L,"Node2"),
        (2L,"Node3"), (3L,"Node4"),(4L,"Node5"),
        (5L,"Node6"), (6L,"Node7"), (7L, "Node8")))

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0), Edge(1, 0, 1.0),
          Edge(0, 2, 1.0), Edge(2, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(1, 2, 1.0), Edge(2, 1, 1.0),
          Edge(1, 3, 1.0), Edge(3, 1, 1.0),
          Edge(2, 3, 1.0), Edge(3, 2, 1.0),
          Edge(3, 4, 1.0), Edge(4, 3, 1.0),
          Edge(4, 5, 1.0), Edge(5, 4, 1.0),
          Edge(4, 6, 1.0), Edge(6, 4, 1.0),
          Edge(4, 7, 1.0), Edge(7, 4, 1.0),
          Edge(5, 6, 1.0), Edge(6, 5, 1.0),
          Edge(5, 7, 1.0), Edge(7, 5, 1.0),
          Edge(6, 7, 1.0), Edge(7, 6, 1.0)
        ))

    /*val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 2, 1.0), Edge(2, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(2, 3, 1.0), Edge(3, 2, 1.0),
          Edge(3, 4, 1.0), Edge(4, 3, 1.0),
          Edge(4, 5, 1.0), Edge(5, 4, 1.0),
          Edge(4, 6, 1.0), Edge(6, 4, 1.0),
          Edge(5, 6, 1.0), Edge(6, 5, 1.0)
        ))*/

    // Build the initial Graph
    val graph = Graph(users, relationships)
    graph.cache()

    /*// Create and RDD for vertices
    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array((0L,"Node1"), (1L,"Node2")))

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0), Edge(1, 0, 2.0), Edge(0, 0, 1.0), Edge(1, 1, 1.0)))

    // Build the initial Graph
    val graph = Graph(users, relationships)*/

    /*toIndexedRowMatrix(graph)
      .rows.sortBy(_.index).collect
      .foreach(row => {
        row.vector.toArray.foreach(v => print("," + v))
        println()
      })*/
    //LabelPropagation(graph, 10)

    // TODO type test for parameters
    val clusters: RDD[Assignment] = MCL.train(graph).assignments
    clusters
      .map(ass => (ass.cluster, ass.id))
      .groupByKey()
      .foreach(cluster =>
        println(cluster._1 + "\n => " + cluster._2.toString())
      )

    // Terminate spark context
    sc.stop()
  }
}