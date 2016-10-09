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

import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.MCLUtils._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** A clustering model for MCL.
  *
  * @see README.md for more details on theory
  *
  * @constructor Constructs an MCL instance with default parameters: {expansionRate: 2, inflationRate: 2, convergenceRate: 0.01, epsilon: 0.05, maxIterations: 10, selfLoopWeight: 0.1, graphOrientationStrategy: "undirected"}.
  * @param expansionRate expansion rate of adjacency matrix at each iteration
  * @param inflationRate inflation rate of adjacency matrix at each iteration
  * @param epsilon pruning parameter. When an edge E1, starting from a node N1, has a weight which percentage is inferior to epsilon regarding other edges Ei starting from N, this weight is set to zero
  * @param maxIterations maximal number of iterations for a non convergent algorithm
  * @param selfLoopWeight a coefficient between 0 and 1 to influence clustering granularity and objective
  * @param graphOrientationStrategy chose a graph strategy completion depending on its nature. 3 choices: undirected, directed, birected.
  */
class MCL private(private var expansionRate: Int,
                  private var inflationRate: Double,
                  private var epsilon: Double,
                  private var maxIterations: Int,
                  private var selfLoopWeight: Double,
                  private var graphOrientationStrategy: String) extends Serializable{

  /** Construct an MCL instance
    *
    * Default parameters: {expansionRate: 2, inflationRate: 2,
    * convergenceRate: 0.01, epsilon: 0.05, maxIterations: 10, selfLoopWeight: 0.1, graphOrientationStrategy: "undirected"}.
    *
    * @return an MCL object
    */
  def this() = this(2, 2.0, 0.01, 10, 0.1, "undirected")

  /** Available graph orientation strategy options.
    *
    * @see README.md for more details
    */
  val graphOrientationStrategyOption: Seq[String] = Seq("undirected", "directed", "bidirected")

  /** Get expansion rate */
  def getExpansionRate: Int = expansionRate

  /** Set the expansion rate.
    *
    * Default: 2.
    *
    * @throws IllegalArgumentException expansionRate must be higher than 1
    */
  def setExpansionRate(expansionRate: Int): MCL = {
    this.expansionRate = expansionRate match {
      case eR if eR > 0 => eR
      case _ => throw new IllegalArgumentException("expansionRate parameter must be higher than 1")
    }
    this
  }

  /** Get inflation rate */
  def getInflationRate: Double = inflationRate

  /** Set the inflation rate.
    *
    * Default: 2.
    *
    * @throws IllegalArgumentException inflationRate must be higher than 0
    */
  def setInflationRate(inflationRate: Double): MCL = {
    this.inflationRate = inflationRate match {
      case iR if iR > 0 => iR
      case _ => throw new IllegalArgumentException("inflationRate parameter must be higher than 0")
    }
    this
  }

  /** Get epsilon coefficient
    *
    * Change an edge value to zero when the overall weight of this edge is less than a certain percentage
    *
    */
  def getEpsilon: Double = epsilon

  /** Set the minimum percentage to get an edge weight to zero.
    *
    * Default: 0.01.
    *
    * @throws IllegalArgumentException epsilon must be higher than 0 and lower than 1
    */
  def setEpsilon(epsilon: Double): MCL = {
    this.epsilon = epsilon match {
      case eps if eps < 1 & eps >= 0 => eps
      case _ => throw new IllegalArgumentException("epsilon parameter must be higher than 0 and lower than 1")
    }

    this
  }

  /** Get stop condition if MCL algorithm does not converge fairly quickly */
  def getMaxIterations: Int = maxIterations

  /** Set maximum number of iterations.
    *
    * Default: 10.
    *
    * @throws IllegalArgumentException maxIterations must be higher than 0
    */
  def setMaxIterations(maxIterations: Int): MCL = {
    this.maxIterations = maxIterations match {
      case mI if mI > 0 => mI
      case _ => throw new IllegalArgumentException("maxIterations parameter must be higher than 0")
    }
    this
  }

  /** Get weight of automatically added self loops in adjacency matrix rows */
  def getSelfLoopWeight: Double = selfLoopWeight

  /** Set self loops weights.
    *
    * Default: 0.1.
    *
    * @throws IllegalArgumentException selfLoopWeight must be higher than 0 and lower than 1
    */
  def setSelfLoopWeight(selfLoopWeight: Double): MCL = {
    this.selfLoopWeight = selfLoopWeight match {
      case slw if slw > 0 & slw <= 1  => slw
      case _ => throw new IllegalArgumentException("selfLoopWeight parameter must be higher than 0 and lower than 1")
    }
    this
  }

  /** Get graph orientation strategy selected depending on graph nature */
  def getGraphOrientationStrategy: String = graphOrientationStrategy

  /** Set graph orientation strategy.
    *
    * Default: undirected.
    *
    * @throws IllegalArgumentException graphOrientationStrategy must be contained in graphOrientationStrategyOption
    */
  def setGraphOrientationStrategy(graphOrientationStrategy: String): MCL = {
    this.graphOrientationStrategy = graphOrientationStrategy match {
      case gos if graphOrientationStrategyOption.contains(gos)  => gos
      case _ => throw new IllegalArgumentException("you must select graphOrientationStrategy option in the following list: " + graphOrientationStrategyOption.mkString(", "))
    }
    this
  }


  /** Normalize matrix
    *
    * @param mat an unnormalized adjacency matrix
    * @return normalized adjacency matrix
    */
  def normalization(mat: IndexedRowMatrix): IndexedRowMatrix ={
    new IndexedRowMatrix(
      mat.rows
        .map{row =>
          val svec = row.vector.toSparse
          IndexedRow(row.index,
            new SparseVector(svec.size, svec.indices, svec.values.map(v => v/svec.values.sum)))
        })
  }

  /** Normalize row
    *
    * @param row an unnormalized row of th adjacency matrix
    * @return normalized row
    */
  def normalization(row: SparseVector): SparseVector ={
    new SparseVector(row.size, row.indices, row.values.map(v => v/row.values.sum))
  }

  /** Remove weakest connections from a row
    *
    * Connections weight in adjacency matrix which is inferior to a very small value is set to 0
    *
    * @param row a row of the adjacency matrix
    * @return sparsed row
    * @todo Add more complex pruning strategies.
    * @see http://micans.org/mcl/index.html
    */
  def removeWeakConnections(row: SparseVector): SparseVector ={
    new SparseVector(
      row.size,
      row.indices,
      row.values.map(v => {
        if(v < epsilon) 0.0
        else v
      })
    )
  }

  /** Expand matrix
    *
    * @param mat an adjacency matrix
    * @return expanded adjacency matrix
    */
  def expansion(mat: IndexedRowMatrix): BlockMatrix = {
    val bmat = mat.toBlockMatrix()
    var resmat = bmat
    for(i <- 1 until expansionRate){
      resmat = resmat.multiply(bmat)
    }
    resmat
  }

  /** Inflate matrix
    *
    * Prune and normalization are applied locally (on each row). So we avoid two more complete scanning of adjacency matrix.
    * As explained in issue #8, pruning is applied on expanded matrix, so we take advantage of natural normalized expansion state.
    *
    * @param mat an adjacency matrix
    * @return inflated adjacency matrix
    */
  def inflation(mat: BlockMatrix): IndexedRowMatrix = {

    new IndexedRowMatrix(
      mat.toIndexedRowMatrix.rows
        .map{row =>
          val svec = removeWeakConnections(row.vector.toSparse) // Pruning elements locally, instead of scanning all matrix again
          IndexedRow(row.index,
            // Normalizing rows locally, instead of scanning all matrix again
            normalization(
              new SparseVector(svec.size, svec.indices, svec.values.map(v => Math.exp(inflationRate*Math.log(v))))
            )
          )
        }
    )
  }

  /** Calculate the distance between two matrices.
    *
    * Find the euclidean distance bewtween two matrices.
    *
    * @param m1 an adjacency matrix at step n
    * @param m2 same adjacency matrix at step n+1
    * @return a normalized distance between m1 and m2
    * @todo Use another object to speed up join between RDD.
    */
  def difference(m1: IndexedRowMatrix, m2: IndexedRowMatrix): Double = {

    val m1RDD:RDD[((Long,Int),Double)] = m1.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })

    val m2RDD:RDD[((Long,Int),Double)] = m2.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })

    val diffRDD = m1RDD.fullOuterJoin(m2RDD).map(diff => Math.pow(diff._2._1.getOrElse(0.0) - diff._2._2.getOrElse(0.0), 2))
    diffRDD.sum()
  }

  /** Train MCL algorithm.
    *
    * @param graph a graph to partitioned
    * @return an MCLModel where each node is associated to one or more clusters
    */
  def run[VD](graph: Graph[VD, Double]): MCLModel = {

    // Add a new attributes to nodes: a unique row index starting from 0 to transform graph into adjacency matrix
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val lookupTable:DataFrame =
      graph.vertices.sortBy(_._1).zipWithIndex()
        .map(indexedVertex => (indexedVertex._2.toInt, indexedVertex._1._1.toInt))
        .toDF("matrixId", "nodeId")

    val preprocessedGraph: Graph[Int, Double] = preprocessGraph(graph, lookupTable)

    val mat = toIndexedRowMatrix(preprocessedGraph, selfLoopWeight, graphOrientationStrategy)

    // Number of current iterations
    var iter = 0
    // Convergence indicator
    var change = 1.0

    var M1:IndexedRowMatrix = normalization(mat)
    while (iter < maxIterations && change > 0) {
      val M2: IndexedRowMatrix = inflation(expansion(M1))
      change = difference(M1, M2)
      iter = iter + 1
      M1 = M2
    }

    // Get attractors in adjacency matrix (nodes with not only null values) and collect every nodes they are attached to in order to form a cluster.

    val rawDF =
      M1.rows.flatMap(
        r => {
          val sv = r.vector.toSparse
          sv.indices.map(i => (r.index, (i, sv.apply(i))))
        }
      ).groupByKey()
       .map(node => (node._1, node._2.maxBy(_._2)._1))
       .toDF("matrixId", "clusterId")

    // Reassign correct ids to each nodes instead of temporary matrix id associated

    val assignments: Dataset[Assignment] =
      rawDF
        .join(lookupTable, rawDF.col("matrixId")===lookupTable.col("matrixId"))
        .select($"nodeId", $"clusterId")
        .map(row => Assignment(row.getInt(0).toLong, row.getInt(1).toLong))

    new MCLModel(assignments)
  }

}

object MCL{

  /** Train an MCL model using the given set of parameters.
    *
    * @param graph training points stored as `BlockMatrix`
    * @param expansionRate expansion rate of adjacency matrix at each iteration
    * @param inflationRate inflation rate of adjacency matrix at each iteration
    * @param epsilon minimum percentage of a weight edge to be significant
    * @param maxIterations maximal number of iterations for a non convergent algorithm
    * @param selfLoopWeight a coefficient between 0 and 1 to influence clustering granularity and objective
    * @param graphOrientationStrategy chose a graph strategy completion depending on its nature. 3 choices: undirected, directed, birected.
    * @return an MCL object
    */
  def train[VD](graph: Graph[VD, Double],
            expansionRate: Int = 2,
            inflationRate: Double = 2.0,
            epsilon : Double = 0.01,
            maxIterations: Int = 10,
            selfLoopWeight: Double = 1,
            graphOrientationStrategy: String = "undirected"): MCLModel = {

    new MCL()
      .setExpansionRate(expansionRate)
      .setInflationRate(inflationRate)
      .setEpsilon(epsilon)
      .setMaxIterations(maxIterations)
      .setSelfLoopWeight(selfLoopWeight)
      .setGraphOrientationStrategy(graphOrientationStrategy)
      .run(graph)
  }

}