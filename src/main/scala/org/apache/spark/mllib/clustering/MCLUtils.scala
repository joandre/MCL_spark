/*The MIT License (MIT)

Copyright (c) 2016, Joan André

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

import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

private[clustering] object MCLUtils {

  def displayMatrix(mat: IndexedRowMatrix): Unit={
    println()
    mat
      .rows.sortBy(_.index).collect()
      .foreach(row => {
        printf(row.index + " => ")
        row.vector.toArray
          .foreach(v => printf(",%.2f", v))
        println()
      })
  }

  //Get a suitable graph for MCL model algorithm: each vertex id in the graph corresponds to a row id in the matrix
  def preprocessGraph(graph: Graph[String, Double], lookupTable: DataFrame): Graph[Int, Double]={
    val newVertices: RDD[(VertexId, Int)] =
      lookupTable.rdd.map(
        row => (row.getInt(1).toLong, row.getInt(0))
      )

    // TODO Beware!!! GroupEdges does not work for self loops

    Graph(newVertices, graph.edges)
      .groupEdges((e1,e2) => e1+e2)
  }

  // Deal with self loop: add one when weight is nil and remain as it is otherwise
  def selfLoopManager(graph: Graph[Int, Double]): RDD[(Int, (Int, Double))] = {

    val selfLoop:RDD[(Int, (Int, Double))] =
      graph
        .triplets
        .filter(e => e.srcId==e.dstId && e.attr > 0)
        .map(e => (e.srcId, e.srcAttr))
        .fullOuterJoin(graph.vertices)
        .filter(join => join._2._1.isEmpty)
        .map(v => (v._2._2.get, (v._2._2.get, 1.0)))

    selfLoop
  }


  //To transform a graph in an IndexedRowMatrix
  def toIndexedRowMatrix(graph: Graph[Int, Double]): IndexedRowMatrix = {

    //TODO No assumptions about a wrong graph format for the moment.
    //Especially relationships values have to be checked before doing what follows
    val rawEntries: RDD[(Int, (Int, Double))] = graph.triplets.map(
      triplet => (triplet.srcAttr, (triplet.dstAttr, triplet.attr))
    )

    val numOfNodes:Int =  graph.numVertices.toInt

    val selfLoop:RDD[(Int, (Int, Double))] = selfLoopManager(graph)
    val entries:RDD[(Int, (Int, Double))] = rawEntries.union(selfLoop)

    val indexedRows = entries.groupByKey().map(e =>
      IndexedRow(e._1, Vectors.sparse(numOfNodes, e._2.toSeq))
    )

    new IndexedRowMatrix(indexedRows, nRows = numOfNodes, nCols = numOfNodes)
  }

  //To transform an IndexedRowMatrix in a graph
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
