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
  def this() = this(2, 2, 0.01,1)

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

  def displayMatrix(mat: IndexedRowMatrix): Unit={
    mat
      .rows.sortBy(_.index).collect()
      .foreach(row => {
        row.vector.toArray.foreach(v => print("," + v))
        println()
      })
  }

  /*
  * TODO add self loop to each node (improvement trick. See: https://www.cs.ucsb.edu/~xyan/classes/CS595D-2009winter/MCL_Presentation2.pdf)
  */

  def selfLoop(mat:IndexedRowMatrix):IndexedRowMatrix ={
    mat
  }

  def normalization(mat: IndexedRowMatrix): IndexedRowMatrix ={
    new IndexedRowMatrix(mat.rows
      .map{row =>
        val svec = row.vector.toSparse
        IndexedRow(row.index,
          new SparseVector(svec.size, svec.indices, svec.values.map(v => v/svec.values.sum)))
      }
    )
  }

  def expansion(mat: IndexedRowMatrix): BlockMatrix = {
    val bmat = mat.toBlockMatrix()
    bmat.multiply(bmat.transpose)
  }

  def inflation(mat: BlockMatrix): IndexedRowMatrix = {

    /*new CoordinateMatrix(mat.toCoordinateMatrix().entries
      .map(entry => MatrixEntry(entry.i, entry.j, Math.exp(inflationRate*Math.log(entry.value))))).toBlockMatrix()*/

    /*new BlockMatrix(mat.blocks.map(b => {
      val denseMat = b._2.toBreeze
      val iter = denseMat.keysIterator
      while (iter.hasNext) {
        val key = iter.next()
        denseMat.update(key, Math.exp(inflationRate * Math.log(denseMat.apply(key))))
      }
      denseMat.toDenseMatrix.toArray
      ((b._1._1, b._1._2), new DenseMatrix(b._2.numCols, b._2.numRows, denseMat.toDenseMatrix.toArray))
    }), mat.rowsPerBlock, mat.colsPerBlock)*/

    /*mat.blocks.foreach(block =>
      block._2.foreachActive((i:Int, j:Int, v:Double) =>
        block._2.update(i ,j , Math.exp(inflationRate * Math.log(v)))))
    mat*/
    new IndexedRowMatrix(
      mat.toIndexedRowMatrix().rows
        .map{row =>
          val svec = row.vector.toSparse
          IndexedRow(row.index,
            new SparseVector(svec.size, svec.indices, svec.values.map(v => Math.exp(inflationRate*Math.log(v)))))
        }
    )

  }

  def difference(m1: IndexedRowMatrix, m2: IndexedRowMatrix): Double = {
    val m1RDD:RDD[Double] = m1.rows.flatMap(r => r.vector.toSparse.values)
    val m2RDD:RDD[Double] = m2.rows.flatMap(r => r.vector.toSparse.values)
    val diffRDD:RDD[Double] = m1RDD.subtract(m2RDD)
    diffRDD.sum()
  }

  /*
   * Train MCL algorithm.
   */
  def run(mat: IndexedRowMatrix): MCLModel = {

    //mat.blocks.foreach(block => println(block._2.foreachActive( (i, j, v) => )))

    //temporaryMatrix = temporaryMatrix.multiply(temporaryMatrix)
    //temporaryMatrix.blocks.foreach(x => println(x.toString()))

    // Number of current iterations
    var iter = 0
    // Convergence indicator
    var change = epsilon + 1

    //TODO Cache adjacency matrix to improve algorithm perfomance
    var M1 = normalization(mat)
    while (iter < maxIterations && change > epsilon) {
      val M2 = normalization(inflation(expansion(M1)))
      change = difference(M1, M2)
      iter = iter + 1
      M1 = M2
    }

    displayMatrix(M1)
    //M1.blocks.map(block => block._2.foreachActive())

    val sc:SparkContext = M1.rows.sparkContext
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
   * @param expansionRate expansion rate of adjacency matrix at each iteration
   * @param inflationRate inflation rate of adjacency matrix at each iteration
   * @param epsilon stop condition for convergence of MCL algorithm
   * @param maxIterations maximal number of iterations for a non convergent algorithm
   */
  def train(graph: Graph[String, Double],
            expansionRate: Double,
            inflationRate: Double,
            epsilon: Double,
            maxIterations: Int): MCLModel = {

    val mat = toIndexedRowMatrix(graph)

    new MCL().setExpansionRate(expansionRate)
      .setInflationRate(inflationRate)
      .setEpsilon(epsilon)
      .setMaxIterations(maxIterations)
      .run(mat)
  }

  /* Trains a MCL model using the default set of parameters.
   *
   * @param graph training points stored as `BlockMatrix`
   */
  def train(graph: Graph[String, Double]): Unit = {

    val mat = toIndexedRowMatrix(graph)

    new MCL().run(mat)
  }

}