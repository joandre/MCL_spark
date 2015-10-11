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
import org.apache.spark.mllib.clustering.MCL
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Define main method (scala entry point)
object Main {

  def main(args: Array[String]) {

    // Initialise spark context
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MCL")
      .set("spark.executor.memory","1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction","1")

    val sc = new SparkContext(conf)

    // Create and RDD for vertices
    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array((0L,"Node1"), (1L,"Node2"),
        (2L,"Node3"), (3L,"Node4"),(4L,"Node5"),
        (5L,"Node6"), (6L,"Node7")))

    // Create an RDD for edges
    /*val relationships: RDD[Edge[Double]] =
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
          Edge(5, 6, 1.0), Edge(6, 5, 1.0)
        ))*/

    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 2, 1.0), Edge(2, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(2, 3, 1.0), Edge(3, 2, 1.0),
          Edge(3, 4, 1.0), Edge(4, 3, 1.0),
          Edge(4, 5, 1.0), Edge(5, 4, 1.0),
          Edge(4, 6, 1.0), Edge(6, 4, 1.0),
          Edge(5, 6, 1.0), Edge(6, 5, 1.0)
        ))

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
    val clusters = MCL.train(graph)

    // Terminate spark context
    sc.stop()
  }
}