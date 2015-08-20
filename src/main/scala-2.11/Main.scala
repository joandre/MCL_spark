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

import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, MatrixEntry, CoordinateMatrix}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.MCL

// Define main method (scala entry point)
object Main {
  //To transform a graph in a coordinate matrix (to add to graphX Graph Class)
  def toCoordinateMatrix(graph: Graph[String, Double]): CoordinateMatrix = {
    //No assumptions about a wrong graph format for the moment.
    //Especially relationships values have to be checked before doing what follows
    val entries: RDD[MatrixEntry] = graph.edges.map(e => MatrixEntry(e.srcId.toLong, e.dstId.toLong, e.attr))
    val mat: CoordinateMatrix = new CoordinateMatrix(entries)

    /*val m = mat.numRows()
    val n = mat.numCols()
    println("\n" + m + "\n" + n)*/
    mat
  }

  //To transform a graph in a block matrix (to add to graphX Graph Class)
  def toBlockMatrix(graph: Graph[String, Double]): BlockMatrix = {
    //No assumptions about a wrong graph format for the moment.
    //Especially reelationships values have to be checked before doing what follows
    val entries: RDD[MatrixEntry] = graph.edges.map(e => MatrixEntry(e.srcId.toLong, e.dstId.toLong, e.attr))
    val mat: CoordinateMatrix = new CoordinateMatrix(entries)

    /*val m = mat.numRows()
    val n = mat.numCols()
    println("\n" + m + "\n" + n)*/
    mat.toBlockMatrix()
  }

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
      sc.parallelize(Array((1L,"Node1"), (2L,"Node2"),
        (3L,"Node3"), (4L,"Node4"),(5L,"Node5"),
        (6L,"Node6"), (7L,"Node7")))

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(1, 2, 1.0), Edge(1, 3, 1.0), Edge(1, 4, 1.0),
        Edge(2, 3, 1.0), Edge(2, 4, 1.0), Edge(2, 5, 1.0),
        Edge(3, 4, 1.0), Edge(5, 6, 1.0), Edge(2, 7, 1.0),
        Edge(6, 7, 1.0)))

    // Build the initial Graph
    val graph = Graph(users, relationships)
    graph.cache()
    val mat: BlockMatrix = toBlockMatrix(graph)

    val clusters = MCL.train(mat)

    // Terminate spark context
    sc.stop()
  }
}