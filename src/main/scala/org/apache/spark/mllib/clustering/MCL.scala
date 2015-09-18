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

package org.apache.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class MCL private(private var expansionRate: Double,
                  private var inflationRate: Double,
                  private var epsilon: Double,
                  private var maxIterations: Int) extends Serializable{

  /*
   * Constructs a MCL instance with default parameters: {expansionRate: 2, inflationRate: 2,
   * epsilon: 0.01, maxIterations: 10}.
   */
  def this() = this(2, 2, 0.01,10)

  /*
   * Expansion rate ...
   */
  def getExpansionRate: Double = expansionRate

  /*
   * Set the expansion rate. Default: 2.
   */
  def setExpansionRate(expansionRate: Double): this.type = {
    this.expansionRate = expansionRate
    this
  }

  /*
   * Inflation rate ...
   */
  def getInflationRate: Double = inflationRate

  /*
   * Set the inflation rate. Default: 2.
   */
  def setInflationRate(inflationRate: Double): this.type = {
    this.inflationRate = inflationRate
    this
  }

  /*
   * Stop condition for convergence of MCL algorithm
   */
  def getEpsilon: Double = epsilon

  /*
   * Set the convergence condition. Default: 0.01.
   */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  /*
   * Stop condition if MCL algorithm does not converge fairly quickly
   */
  def getEMaxIterations: Double = maxIterations

  /*
   * Set maximum number of iterations. Default: 10.
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /*
  * TODO add self loop to each node (improvement trick. See: https://www.cs.ucsb.edu/~xyan/classes/CS595D-2009winter/MCL_Presentation2.pdf)
  */

  def selfLoop(mat:BlockMatrix):BlockMatrix ={
    mat
  }

  def normalization(mat: BlockMatrix): BlockMatrix ={
    new IndexedRowMatrix(mat.transpose.toIndexedRowMatrix().rows.map(row =>
      IndexedRow(row.index, new DenseVector(row.vector.toSparse.values.map(v => v/row.vector.toArray.sum)))
    )).toBlockMatrix().transpose
  }

  def normalization(mat: IndexedRowMatrix): IndexedRowMatrix ={
    new IndexedRowMatrix(mat.rows.map(row =>
      IndexedRow(row.index, new DenseVector(row.vector.toSparse.values.map(v => v/row.vector.toArray.sum)))
    )).toBlockMatrix().transpose
  }

  def expansion(mat: BlockMatrix): BlockMatrix = {
    mat.multiply(mat)
  }

  def inflation(mat: BlockMatrix): BlockMatrix ={
    /*new CoordinateMatrix(mat.entries
      .map(entry => MatrixEntry(entry.i, entry.j, Math.exp(inflationRate*Math.log(entry.value)))))*/
    new CoordinateMatrix(mat.toCoordinateMatrix().entries
      .map(entry => MatrixEntry(entry.i, entry.j, Math.exp(inflationRate*Math.log(entry.value))))).toBlockMatrix()
  }

  def difference(m1: BlockMatrix, m2: BlockMatrix): Double = {
    val m1RDD:RDD[Double] = m1.toCoordinateMatrix().entries.map(e => e.value)
    val m2RDD:RDD[Double] = m2.toCoordinateMatrix().entries.map(e => e.value)
    val diffRDD:RDD[Double] = m1RDD.subtract(m2RDD)
    diffRDD.sum()
  }

  /*
   * Train MCL algorithm.
   */
  def run(mat: IndexedRowMatrix,
           sc: SparkContext): MCLModel = {

    //mat.blocks.foreach(block => println(block._2.foreachActive( (i, j, v) => )))

    //temporaryMatrix = temporaryMatrix.multiply(temporaryMatrix)
    //temporaryMatrix.blocks.foreach(x => println(x.toString()))

    // Number of current iterations
    var iter = 0
    // Convergence indicator
    var change = epsilon + 1

    var M1 = normalization(mat)
    while (iter < maxIterations && change > epsilon) {
      val M2 = normalization(inflation(expansion(M1)))
      change = difference(M1, M2)
      iter = iter + 1
      M1 = M2
    }

//    displayMatrix(M1)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val tempArray = sc.parallelize(Array((1,1))).toDF()
    val assignmentsRDD: RDD[Assignment] = tempArray.map{
      case Row(id: Long, cluster: Int) => Assignment(id, cluster)
    }

    new MCLModel(this.expansionRate, this.inflationRate, this.epsilon, this.maxIterations, assignmentsRDD)
  }

}

object MCL{

  def displayMatrix(mat: IndexedRowMatrix): Unit={
    mat
      .rows.sortBy(_.index).collect
      .foreach(row => {
      row.vector.toArray.foreach(v => print("," + v))
      println()
    })
  }

  //To transform a graph in a block matrix (to add to graphX Graph Class)
  def toBlockMatrix(graph: Graph[String, Double]): BlockMatrix = {
    //No assumptions about a wrong graph format for the moment.
    //Especially relationships values have to be checked before doing what follows
    val entries: RDD[MatrixEntry] = graph.edges.map(
      e => {
        println(e.srcId.toLong + "," + e.dstId.toLong + " => " + e.attr)
        MatrixEntry(e.srcId.toLong, e.dstId.toLong, e.attr)
      }
    )

    new CoordinateMatrix(entries, nRows=graph.numVertices, nCols=graph.numVertices).toBlockMatrix
  }

  //To transform a graph in an indexed row matrix (row add to graphX Graph Class)
  def toIndexedRowMatrix(graph: Graph[String, Double]): IndexedRowMatrix = {
    //No assumptions about a wrong graph format for the moment.
    //Especially relationships values have to be checked before doing what follows
    val entries: RDD[(Int, (Int, Double))] = graph.edges.map(
      e => (e.srcId.toInt, (e.dstId.toInt, e.attr))
    )

    val numOfNodes:Int =  graph.numVertices.toInt

    val indexedRows = entries.groupByKey().map(e =>
      IndexedRow(e._1, Vectors.sparse(numOfNodes, e._2.toSeq))
    )

    new IndexedRowMatrix(indexedRows, nRows = numOfNodes, nCols = numOfNodes)
  }

  /*
   * Trains a MCL model using the given set of parameters.
   *
   * @param graph training points stored as `BlockMatrix`
   * @param expansionRate expansion rate of adjency matrix at each iteration
   * @param inflationRate inflation rate of adjency matrix at each iteration
   * @param epsilon stop condition for convergence of MCL algorithm
   * @param maxIterations maximal number of iterations for a non convergent algorithm
   * @param sc current Spark Context
   */
  def train(graph: Graph[String, Double],
            expansionRate: Double,
            inflationRate: Double,
            epsilon: Double,
            maxIterations: Int,
            sc: SparkContext): MCLModel = {

    val mat = toIndexedRowMatrix(graph)

    new MCL().setExpansionRate(expansionRate)
      .setInflationRate(inflationRate)
      .setEpsilon(epsilon)
      .setMaxIterations(maxIterations)
      .run(mat, sc)
  }

  /* Trains a MCL model using the default set of parameters.
   *
   * TODO Check how to deal properly with sparkContext
   *
   * @param graph training points stored as `BlockMatrix`
   */
  def train(graph: Graph[String, Double],
            sc: SparkContext): MCLModel = {

    val mat = toIndexedRowMatrix(graph)

    //TODO Check toBlockMatrix effects on IndexedRowMatrix
    //new MCL().run(mat.toBlockMatrix, sc)
    new MCL().run(mat, sc)
  }

}