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

import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.MCLUtils._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.io.Source

/** Scala Tests class for MCLUtils functions */
class MCLUtilsSuite extends MCLFunSuite{

  // Disable Spark messages when running program
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Unit Tests

  test("Print functions", UnitTest){
    val indexedMatrix: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(1,0,0,0,1,0))),
            IndexedRow(1, new DenseVector(Array(0,1,1,0,1,1))),
            IndexedRow(2, new DenseVector(Array(0,1,1,0,0,1))),
            IndexedRow(3, new DenseVector(Array(0,0,0,1,0,1))),
            IndexedRow(4, new DenseVector(Array(1,1,0,0,1,0))),
            IndexedRow(5, new DenseVector(Array(0,1,1,1,0,1)))
          )
        ))

    // Force local number format so "." is the only separator used for float numbers in print tests no matter in which environment they run
    Locale.setDefault(new Locale("en", "US"))

    val streamIM = new java.io.ByteArrayOutputStream()
    Console.withOut(streamIM) {
      displayMatrix(indexedMatrix)
    }

    streamIM.toString shouldEqual "\n0 => ,1.0000,0.0000,0.0000,0.0000,1.0000,0.0000\n1 => ,0.0000,1.0000,1.0000,0.0000,1.0000,1.0000\n2 => ,0.0000,1.0000,1.0000,0.0000,0.0000,1.0000\n3 => ,0.0000,0.0000,0.0000,1.0000,0.0000,1.0000\n4 => ,1.0000,1.0000,0.0000,0.0000,1.0000,0.0000\n5 => ,0.0000,1.0000,1.0000,1.0000,0.0000,1.0000\n"

    val streamBM = new java.io.ByteArrayOutputStream()
    Console.withOut(streamBM) {
      displayBlockMatrix(indexedMatrix.toBlockMatrix)
    }

    streamBM.toString shouldEqual "\n6 x 6 CSCMatrix\n(0,0) 1.0\n(4,0) 1.0\n(1,1) 1.0\n(2,1) 1.0\n(4,1) 1.0\n(5,1) 1.0\n(1,2) 1.0\n(2,2) 1.0\n(5,2) 1.0\n(3,3) 1.0\n(5,3) 1.0\n(0,4) 1.0\n(1,4) 1.0\n(4,4) 1.0\n(1,5) 1.0\n(2,5) 1.0\n(3,5) 1.0\n(5,5) 1.0"

  }

  test("Preprocessing Graph (ordered id for vertices and remove multiple edges)", UnitTest){

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val matchingList: RDD[(Int,Int)] = sc.parallelize(Array((0,2), (1,1), (2,3), (3,5), (4,8), (5, 0)))
    val lookupTable: DataFrame = matchingList.toDF("matrixId", "nodeId")

    // Create and RDD for vertices
    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array((0L,"Node5"), (1L,"Node1"),
        (2L, "Node0"), (3L,"Node2"), (5L,"Node3"),(8L,"Node4")))

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0), Edge(1, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(0, 5, 1.0), Edge(5, 0, 1.0),
          Edge(1, 3, 1.0), Edge(3, 1, 1.0),
          Edge(1, 8, 1.0), Edge(8, 1, 1.0),
          Edge(2, 8, 1.0), Edge(8, 2, 1.0),
          Edge(8, 2, 1.0), Edge(2, 2, 1.0),
          Edge(2, 2, 1.0)
        ))

    // Build the initial Graph
    val graph = Graph(users, relationships)

    val cleanedGraph: Graph[Int, Double] = preprocessGraph(graph, lookupTable)

    // Create and RDD for vertices
    val challengeUsers: RDD[(VertexId, Int)] =
      sc.parallelize(Array((2L,0), (1L,1),
        (3L,2), (5L,3), (8L,4), (0L,5)))

    // Create an RDD for edges
    val challengeRelationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0), Edge(1, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(0, 5, 1.0), Edge(5, 0, 1.0),
          Edge(1, 3, 1.0), Edge(3, 1, 1.0),
          Edge(1, 8, 1.0), Edge(8, 1, 1.0),
          Edge(2, 8, 1.0), Edge(8, 2, 2.0),
          Edge(2, 2, 2.0)
        ))

    // Build the initial Graph
    val challengeGraph = Graph(challengeUsers, challengeRelationships)

    cleanedGraph.vertices.count shouldEqual challengeGraph.vertices.count
    cleanedGraph.vertices.map(v => (v._1, v._2)).collect.sorted shouldEqual challengeGraph.vertices.map(v => (v._1, v._2)).collect.sorted

    /*cleanedGraph.edges
      .map(v => ((v.srcId, v.dstId), v.attr))
      .collect.sortBy(tup => tup._1) shouldEqual
    challengeGraph.edges
      .map(v => ((v.srcId, v.dstId), v.attr))
      .collect.sortBy(tup => tup._1)*/

  }

  test("Add self loop too each nodes", UnitTest){

    // Create and RDD for vertices
    val users: RDD[(VertexId, Int)] =
      sc.parallelize(Array((2L,0), (1L,1),
        (3L,2), (5L,3), (8L,4), (0L,5)))

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 2.0), Edge(1, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(0, 5, 1.0), Edge(5, 0, 1.0),
          Edge(1, 3, 1.0), Edge(3, 1, 1.0),
          Edge(1, 8, 1.0), Edge(8, 1, 1.0),
          Edge(2, 8, 1.0), Edge(8, 2, 1.0),
          Edge(2, 2, 1.0)
        ))

    // Build the initial Graph
    val graph = Graph(users, relationships)

    val edgesWithSelfLoops: RDD[(Int, (Int, Double))] = selfLoopManager(graph, 2)

    val objective: RDD[(Int, (Int, Double))] =
      sc.parallelize(
        Seq((1, (1, 4.0)), (2, (2, 2.0)),
          (3, (3, 2.0)), (4, (4, 2.0)),
          (5, (5, 4.0))
        ))

    edgesWithSelfLoops.count shouldEqual objective.count
    edgesWithSelfLoops.collect.sortBy(edge => (edge._1, edge._2)) shouldEqual objective.collect.sortBy(edge => (edge._1, edge._2))

  }

  test("Completion strategy for graph depending on its nature (oriented or not)", UnitTest){

    // For undirected graphs
    // Create and RDD for vertices
    val undirectedUsers: RDD[(VertexId, Int)] =
      sc.parallelize(Array((2L,0), (1L,1),
        (3L,2), (5L,3), (8L,4), (0L,5)))

    // Create an RDD for edges
    val undirectedRelationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0), Edge(1, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(0, 5, 1.0), Edge(5, 0, 1.0),
          Edge(1, 3, 1.0), Edge(3, 1, 1.0),
          Edge(1, 8, 1.0), Edge(8, 1, 1.0),
          Edge(2, 8, 1.0), Edge(8, 2, 1.0),
          Edge(2, 2, 1.0)
        ))

    // Build the initial Graph
    val undirectedGraph = Graph(undirectedUsers, undirectedRelationships)

    val undirectedEdges: RDD[(Int, (Int, Double))] = graphOrientationManager(undirectedGraph, "undirected")

    // For directed graphs
    // Create and RDD for vertices
    val directedUsers: RDD[(VertexId, Int)] =
      sc.parallelize(Array((2L,0), (1L,1),
        (3L,2), (5L,3), (8L,4), (0L,5)))

    // Create an RDD for edges
    val directedRelationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0),
          Edge(0, 3, 1.0),
          Edge(0, 5, 1.0),
          Edge(1, 3, 1.0),
          Edge(1, 8, 1.0),
          Edge(2, 8, 1.0),
          Edge(2, 2, 1.0)
        ))

    // Build the initial Graph
    val directedGraph = Graph(directedUsers, directedRelationships)

    val directedEdges: RDD[(Int, (Int, Double))] = graphOrientationManager(directedGraph, "directed")

    // For bidirected graphs
    // Create and RDD for vertices
    val bidirectedUsers: RDD[(VertexId, Int)] =
      sc.parallelize(Array((2L,0), (1L,1),
        (3L,2), (5L,3), (8L,4), (0L,5)))

    // Create an RDD for edges
    val bidirectedRelationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0),
          Edge(0, 3, 1.0),
          Edge(0, 5, 1.0),
          Edge(1, 3, 1.0), Edge(3, 1, 1.0),
          Edge(1, 8, 1.0), Edge(8, 1, 1.0),
          Edge(2, 8, 1.0),
          Edge(2, 2, 1.0)
        ))

    // Build the initial Graph
    val bidirectedGraph = Graph(bidirectedUsers, bidirectedRelationships)

    val bidirectedEdges: RDD[(Int, (Int, Double))] = graphOrientationManager(bidirectedGraph, "bidirected")

    val objective: RDD[(Int, (Int, Double))] =
      sc.parallelize(
        Seq((5, (1, 1.0)), (1, (5, 1.0)),
          (5, (2, 1.0)), (2, (5, 1.0)),
          (5, (3, 1.0)), (3, (5, 1.0)),
          (1, (2, 1.0)), (2, (1, 1.0)),
          (1, (4, 1.0)), (4, (1, 1.0)),
          (0, (4, 1.0)), (4, (0, 1.0)),
          (0, (0, 1.0))
        ))

    undirectedEdges.count shouldEqual objective.count
    undirectedEdges.collect.sortBy(edge => (edge._1, edge._2)) shouldEqual objective.collect.sortBy(edge => (edge._1, edge._2))
    directedEdges.count shouldEqual objective.count
    directedEdges.collect.sortBy(edge => (edge._1, edge._2)) shouldEqual objective.collect.sortBy(edge => (edge._1, edge._2))
    bidirectedEdges.count shouldEqual objective.count
    bidirectedEdges.collect.sortBy(edge => (edge._1, edge._2)) shouldEqual objective.collect.sortBy(edge => (edge._1, edge._2))

  }

  // Integration Tests

  test("Adjacency Matrix Transformation", IntegrationTest) {

    // Load data
    val source:Seq[String] = Source.fromURL(getClass.getResource("/MCLUtils/OrientedEdges.txt")).getLines().toSeq
    val nodesFile:Seq[String] = Source.fromURL(getClass.getResource("/MCLUtils/OrientedNodes.txt")).getLines().toSeq
    val matrixSelfLoop:Seq[String] = Source.fromURL(getClass.getResource("/MCLUtils/OrientedMatrixSelfLoop.txt")).getLines().toSeq

    val edges:RDD[Edge[Double]] =
      sc.parallelize(
        source
        .map(l => l.split(" "))
        .map(e => Edge(e(0).toLong, e(1).toLong, e(2).toDouble))
      )
    val nodes:RDD[(VertexId, String)] =
      sc.parallelize(
        nodesFile
          .map(l => l.split(" "))
          .map(e => (e(0).toLong, "default"))
      )

    val graph = Graph(nodes, edges)

    var range:Long = 0
    val initialMatrixWithSelLoop =
      new IndexedRowMatrix(
        sc.parallelize(
          matrixSelfLoop
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

    //Prepare graph for transformation

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val lookupTable:DataFrame =
      graph.vertices.sortByKey().zipWithIndex()
        .map(indexedVertice => (indexedVertice._2.toInt, indexedVertice._1._1.toInt, indexedVertice._1._2))
        .toDF("matrixId", "nodeId", "attribute")

    val preprocessedGraph: Graph[Int, Double] = preprocessGraph(graph, lookupTable)

    //Test matrix transformation

    val adjacencyMat:IndexedRowMatrix = toIndexedRowMatrix(preprocessedGraph, 1.0, "undirected")

    adjacencyMat.numRows shouldEqual initialMatrixWithSelLoop.numRows
    adjacencyMat.numCols shouldEqual initialMatrixWithSelLoop.numCols
    initialMatrixWithSelLoop.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      .join(
        adjacencyMat.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      )
      .collect.foreach(
        pairOfRows =>
          {
            pairOfRows._2._1 shouldEqual pairOfRows._2._2
          }
      )

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

  }

}
