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
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class MCL private(private var expansionRate: Double,
                  private var inflationRate: Double,
                  private var epsilon: Double,
                  private var maxIterations: Int) extends Serializable{

  /**
   * Constructs a MCL instance with default parameters: {expansionRate: 2, inflationRate: 2,
   * epsilon: 0.01, maxIterations: 10}.
   */
  def this() = this(2, 2, 0.01,10)

  /**
   * Expansion rate ...
   */
  def getExpansionRate: Double = expansionRate

  /**
   * Set the expansion rate. Default: 2.
   */
  def setExpansionRate(expansionRate: Double): this.type = {
    this.expansionRate = expansionRate
    this
  }

  /**
   * Inflation rate ...
   */
  def getInflationRate: Double = inflationRate

  /**
   * Set the inflation rate. Default: 2.
   */
  def setInflationRate(inflationRate: Double): this.type = {
    this.inflationRate = inflationRate
    this
  }

  /**
   * Stop condition for convergence of MCL algorithm
   */
  def getEpsilon: Double = epsilon

  /**
   * Set the convergence condition. Default: 0.01.
   */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  /**
   * Stop condition if MCL algorithm does not converge fairly quickly
   */
  def getEMaxIterations: Double = maxIterations

  /**
   * Set maximum number of iterations. Default: 10.
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  def normalization(mat: BlockMatrix): BlockMatrix ={
    new IndexedRowMatrix(mat.transpose.toIndexedRowMatrix().rows.map(row =>
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
    m1.blocks.map(b => b._1._1)
    0.0
  }

  /**
   * Train MCL algorithm.
   */
  def run(mat: BlockMatrix): MCLModel = {

    //mat.blocks.foreach(block => println(block._2.foreachActive( (i, j, v) => )))

    //temporaryMatrix = temporaryMatrix.multiply(temporaryMatrix)
    //temporaryMatrix.blocks.foreach(x => println(x.toString()))

    // Number of current iterations
    val iter = 0
    // Convergence indicator
    var change = epsilon + 1

    var M1 = normalization(mat)
    while (iter < maxIterations && change > epsilon) {
      val M2 = normalization(inflation(expansion(M1)))
      change = difference(M1, M2)
      M1 = M2
    }

    val sc = new SparkContext()
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

  //To transform a graph in a coordinate matrix (to add to graphX Graph Class)
  def toCoordinateMatrix(graph: Graph[String, Double]): CoordinateMatrix = {
    //No assumptions about a wrong graph format for the moment.
    //Especially relationships values have to be checked before doing what follows
    val entries: RDD[MatrixEntry] = graph.edges.map(e => MatrixEntry(e.srcId.toLong, e.dstId.toLong, e.attr))

    new CoordinateMatrix(entries)
  }

  //To transform a graph in a block matrix (to add to graphX Graph Class)
  def toBlockMatrix(graph: Graph[String, Double]): BlockMatrix = {
    //No assumptions about a wrong graph format for the moment.
    //Especially relationships values have to be checked before doing what follows
    val entries: RDD[MatrixEntry] = graph.edges.map(e => MatrixEntry(e.srcId.toLong, e.dstId.toLong, e.attr))

    new CoordinateMatrix(entries).toBlockMatrix
  }

  /**
   * Trains a MCL model using the given set of parameters.
   *
   * @param graph training points stored as `BlockMatrix`
   * @param expansionRate expansion rate of adjency matrix at each iteration
   * @param inflationRate inflation rate of adjency matrix at each iteration
   * @param epsilon stop condition for convergence of MCL algorithm
   * @param maxIterations maximal number of iterations for a non convergent algorithm
   */
  def train(graph: Graph[String, Double],
            expansionRate: Double,
            inflationRate: Double,
            epsilon: Double,
            maxIterations: Int): MCLModel = {

    val mat = toBlockMatrix(graph)

    new MCL().setExpansionRate(expansionRate)
      .setInflationRate(inflationRate)
      .setEpsilon(epsilon)
      .setMaxIterations(maxIterations)
      .run(mat)
  }

  /**
   * Trains a MCL model using the default set of parameters.
   *
   * @param graph training points stored as `BlockMatrix`
   */
  def train(graph: Graph[String, Double]): MCLModel = {

    val mat = toBlockMatrix(graph)

    new MCL().run(mat)
  }

}