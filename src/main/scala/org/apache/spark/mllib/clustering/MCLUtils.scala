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

import breeze.linalg.max
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Utils functions for MCL algorithm implementation.
  */
object MCLUtils {

  /** Print an adjacency matrix in nice format.
    *
    * @param mat an adjacency matrix
    */
  def displayMatrix(mat: IndexedRowMatrix): Unit={
    println()
    mat
      .rows.sortBy(_.index).collect()
      .foreach(row => {
        printf(row.index + " => ")
        row.vector.toArray
          .foreach(v => printf(",%.4f", v))
        println()
      })
  }

  def displayBlockMatrix(mat: BlockMatrix): Unit={
    println()
    mat
      .blocks.sortBy(_._1).collect()
      .foreach(
        block => {
          printf(block._2.toString())
      })
  }

  /** Get a suitable graph for MCL model algorithm.
    *
    * Each vertex id in the graph corresponds to a row id in the adjacency matrix.
    *
    * @param graph original graph
    * @param lookupTable a matching table with nodes ids and new ordered ids
    * @return prepared graph for MCL algorithm
    */
  def preprocessGraph[VD](graph: Graph[VD, Double], lookupTable: DataFrame): Graph[Int, Double]={
    val newVertices: RDD[(VertexId, Int)] =
      lookupTable.rdd.map(
        row => (row.getInt(1).toLong, row.getInt(0))
      )

    Graph(newVertices, graph.edges)
      .groupEdges((e1,e2) => e1 + e2)
  }

  /** Deal with self loop
    *
    * Add one when weight is nil and remain as it is otherwise
    *
    * @param graph original graph
    * @param selfLoopWeight a coefficient between 0 and 1 to influence clustering granularity and objective
    * @return an RDD of self loops weights and associated coordinates.
    */
  def selfLoopManager(graph: Graph[Int, Double], selfLoopWeight: Double): RDD[(Int, (Int, Double))] = {

    val graphWithLinkedEdges: Graph[Array[Edge[Double]], Double] =
      Graph(
        graph
          .collectEdges(EdgeDirection.Either),
        graph.edges
      )

    val selfLoop:RDD[(Int, (Int, Double))] =
      graph
      .triplets
      .filter(e => e.srcId==e.dstId && e.attr > 0)
      .map(e => (e.srcId, e.srcAttr))
      .fullOuterJoin(graph.vertices)
      .filter(join => join._2._1.isEmpty)
      .leftOuterJoin(graphWithLinkedEdges.vertices)
      .map(v =>
        (v._2._1._2.get,
          (v._2._1._2.get,
            v._2._2.getOrElse(Array(Edge(1.0.toLong, 1.0.toLong, 1.0))).map(e => e.attr).max*selfLoopWeight)
          )
      )

    selfLoop
  }

  /** Deal with multiple adjacency matrix filling strategy depending on graph orientation
    *
    * @param graph original graph
    * @param graphOrientationStrategy chose a graph strategy completion depending on its nature. 3 choices: undirected, directed, birected.
    * @return an RDD of new edges weights and associated coordinates.
    */
  def graphOrientationManager(graph: Graph[Int, Double], graphOrientationStrategy: String): RDD[(Int, (Int, Double))] = {

    graphOrientationStrategy match {

      //Undirected Graph Solution
      case "undirected" =>

        graph.triplets.map(
          triplet => (triplet.srcAttr, (triplet.dstAttr, triplet.attr))
        )

      //Directed Graph Solution => with only one possible orientation per edge
      case "directed" =>

        graph.triplets.flatMap(
          triplet => {
            if (triplet.srcAttr != triplet.dstAttr) {
              Array((triplet.srcAttr, (triplet.dstAttr, triplet.attr)), (triplet.dstAttr, (triplet.srcAttr, triplet.attr)))
            }
            else {
              Array((triplet.srcAttr, (triplet.dstAttr, triplet.attr)))
            }
          }
        )

      //Directed Graph Solution => with only one possible orientation per edge
      case "bidirected" =>

        val tempEntries: RDD[((Int, Int), (Double, Int))] = graph.triplets.flatMap(
          triplet => {
            Array(
              ((triplet.srcAttr, triplet.dstAttr), (triplet.attr, 1)),
              ((triplet.dstAttr, triplet.srcAttr), (triplet.attr, 2))
            )
          }
        )

        tempEntries
          .groupByKey()
          .map(
            e =>
              if(e._2.size > 1){
                val value = e._2.filter(v => v._2 == 1).head._1
                (e._1._1, (e._1._2, value))
              }
              else{
                (e._1._1, (e._1._2, e._2.head._1))
              }
          )
    }
  }

  /** Transform a Graph into an IndexedRowMatrix
    *
    * @param graph original graph
    * @param selfLoopWeight a coefficient between 0 and 1 to influence clustering granularity and objective
    * @param graphOrientationStrategy chose a graph strategy completion depending on its nature. 3 choices: undirected, directed, birected.
    * @return a ready adjacency matrix for MCL process.
    * @todo Check graphOrientationStrategy choice for current graph
    */
  def toIndexedRowMatrix(graph: Graph[Int, Double], selfLoopWeight: Double, graphOrientationStrategy: String): IndexedRowMatrix = {

    //Especially relationships values have to be checked before doing what follows
    val rawEntries: RDD[(Int, (Int, Double))] = graphOrientationManager(graph, graphOrientationStrategy)

    val numOfNodes:Int =  graph.numVertices.toInt

    val selfLoop:RDD[(Int, (Int, Double))] = selfLoopManager(graph, selfLoopWeight)
    val entries:RDD[(Int, (Int, Double))] = rawEntries.union(selfLoop)

    val indexedRows = entries.groupByKey().map(e =>
      IndexedRow(e._1, Vectors.sparse(numOfNodes, e._2.toSeq))
    )

    new IndexedRowMatrix(indexedRows)
  }

  /** Transform an IndexedRowMatrix into a Graph
    *
    * @param mat an adjacency matrix
    * @param vertices vertices of original graph
    * @return associated graph
    */
  def toGraph(mat: IndexedRowMatrix, vertices: RDD[(VertexId, String)]): Graph[String, Double] = {
    val edges: RDD[Edge[Double]] =
      mat.rows.flatMap(f = row => {
        val svec: SparseVector = row.vector.toSparse
        val it:Range = svec.indices.indices
        it.map(ind => Edge(row.index, svec.indices.apply(ind), svec.values.apply(ind)))
      })
    Graph(vertices, edges)
  }



}
