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

package org.apache.spark.mllib.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.MCLUtils._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

class MCLUtilsSuite extends MCLFunSuite{

  // Disable Spark messages when running programm
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  test("Adjacency Matrix Transformation") {

    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    // Load data
    val source:Seq[String] = Source.fromURL(getClass.getResource("/OrientedEdges.txt")).getLines().toSeq
    val nodes:Seq[String] = Source.fromURL(getClass.getResource("/OrientedNodes.txt")).getLines().toSeq
    val matrix:Seq[String] = Source.fromURL(getClass.getResource("/OrientedMatrix.txt")).getLines().toSeq
    val matrixSelfLoop:Seq[String] = Source.fromURL(getClass.getResource("/OrientedMatrixSelfLoop.txt")).getLines().toSeq

    val edges:RDD[Edge[Double]] =
      sc.parallelize(
        source
        .map(l => l.split(" "))
        .map(e => Edge(e(0).toLong, e(1).toLong, e(2).toDouble))
      )
    val links:RDD[(VertexId, String)] =
      sc.parallelize(
        nodes
          .map(l => l.split(" "))
          .map(e => (e(0).toLong, "default"))
      )

    val graph = Graph(links, edges)

    var range:Long = 0
    val initialMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          matrix
            .map{
              line =>
                range = range + 1
                new IndexedRow(
                  range-1,
                  new DenseVector(
                    line.split(";").map(e => e.toDouble)
                  )
                )
            }
        )
      )
    var range2:Long = 0
    val initialMatrixWithSelLoop =
      new IndexedRowMatrix(
        sc.parallelize(
          matrixSelfLoop
            .map{
              line =>
                range2 = range2 + 1
                new IndexedRow(
                  range2-1,
                  new DenseVector(
                    line.split(",").map(e => e.toDouble)
                  )
                )
            }
        )
      )

    //Prepare graph for transformation

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val lookupTable:DataFrame =
      graph.vertices.sortByKey().zipWithIndex()
        .map(indexedVertice => (indexedVertice._2.toInt, indexedVertice._1._1.toInt, indexedVertice._1._2))
        .toDF("matrixId", "nodeId", "attribute")

    val preprocessedGraph: Graph[Int, Double] = preprocessGraph(graph, lookupTable)

    //Test matrix transformation

    val adjacencyMat:IndexedRowMatrix = toIndexedRowMatrix(preprocessedGraph)

    adjacencyMat.numRows shouldEqual initialMatrixWithSelLoop.numRows
    adjacencyMat.numCols shouldEqual initialMatrixWithSelLoop.numCols
    initialMatrixWithSelLoop.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      .join(
        adjacencyMat.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      )
      .collect.foreach(pairOfRows => pairOfRows._2._1 shouldEqual pairOfRows._2._2)

    //Test transformation from adjacency matrix to graph

    val vertices:RDD[(VertexId, String)] = lookupTable.rdd.map(row => (row.getInt(0).toLong, row.getString(2)))
    val resultGraph: Graph[String, Double] = toGraph(adjacencyMat, vertices)

    // Missing self edges are manually added
    val preEdges = preprocessedGraph.triplets
      .map(tri => Edge(tri.srcAttr, tri.dstAttr, tri.attr)).collect
      .union(for (i <- 1 to (preprocessedGraph.vertices.count.toInt - 2)) yield Edge(i, i , 1.0))
      .sortBy(e => (e.srcId, e.dstId))

    val postEdges = resultGraph.edges.collect.sortBy(e => (e.srcId, e.dstId))

    preprocessedGraph.vertices.count shouldEqual resultGraph.vertices.count
    preEdges.toSeq.length shouldEqual postEdges.toSeq.length
    preEdges.toSeq shouldEqual postEdges.toSeq

    //Test self loop manager

    val selfLoop: RDD[(Int, (Int, Double))] = selfLoopManager(preprocessedGraph)
    selfLoop.count shouldEqual 98

  }

}
