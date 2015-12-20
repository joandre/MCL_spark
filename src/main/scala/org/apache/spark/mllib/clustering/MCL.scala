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

//TODO Check every svec (collect of each row in the driver)
class MCL private(private var expansionRate: Int,
                  private var inflationRate: Double,
                  private var convergenceRate: Double,
                  private var epsilon: Double,
                  private var maxIterations: Int) extends Serializable{

  /*
  * Constructs an MCL instance with default parameters: {expansionRate: 2, inflationRate: 2,
  * convergenceRate: 0.01, epsilon: 0.05, maxIterations: 10}.
  */

  def this() = this(2, 2.0, 0.01, 0.01, 10)

  /*
   * Expansion rate
   */
  def getExpansionRate: Int = expansionRate

  /*
   * Set the expansion rate. Default: 2.
   */
  def setExpansionRate(expansionRate: Int): this.type = {
    this.expansionRate = expansionRate match {
      case eR if eR > 0 => eR
      case _ => throw new Exception("expansionRate parameter must be higher than 1")
    }
    this
  }

  /*
   * Inflation rate
   */
  def getInflationRate: Double = inflationRate

  /*
   * Set the inflation rate. Default: 2.
   */
  def setInflationRate(inflationRate: Double): this.type = {
    this.inflationRate = inflationRate match {
      case iR if iR > 0 => iR
      case _ => throw new Exception("inflationRate parameter must be higher than 0")
    }
    this
  }

  /*
   * Stop condition for convergence of MCL algorithm
   */
  def getConvergenceRate: Double = convergenceRate

  /*
   * Set the convergence condition. Default: 0.01.
   */
  def setConvergenceRate(convergenceRate: Double): this.type = {
    this.convergenceRate = convergenceRate match {
      case cR if cR < 1 & cR > 0 => cR
      case _ => throw new Exception("convergenceRate parameter must be higher than 0 and lower than 1")
    }
    this
  }

  /*
   * Change an edge value to zero when the overall weight of this edge is less than a certain percentage
   */
  def getEpsilon: Double = convergenceRate

  /*
   * Set the minimum percentage to get an edge weigth to zero. Default: 0.05.
   */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon match {
      case eps if eps < 1 & eps >= 0 => eps
      case _ => throw new Exception("epsilon parameter must be higher than 0 and lower than 1")
    }

    this
  }

  /*
   * Stop condition if MCL algorithm does not converge fairly quickly
   */
  def getMaxIterations: Int = maxIterations

  /*
   * Set maximum number of iterations. Default: 10.
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations match {
      case mI if mI > 0 => mI
      case _ => throw new Exception("maxIterations parameter must be higher than 0")
    }
    this
  }

  def displayMatrix(mat: IndexedRowMatrix): Unit={
    println()
    mat
      .rows.sortBy(_.index).collect()
      .foreach(row => {
        row.vector.toArray
          .foreach(v => printf(",%.2f", v))
        println()
      })
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

  // TODO Check expansion calculation (especially power of a matrix) See https://en.wikipedia.org/wiki/Exponentiation_by_squaring for an improvement.
  def expansion(mat: IndexedRowMatrix): BlockMatrix = {
    var bmat = mat.toBlockMatrix()
    for(i <- 1 to (expansionRate-1)){
      bmat = bmat.multiply(bmat)
    }
    bmat
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

  //Remove weakest connections from the graph (which connections weight in adjacency matrix is inferior to a very small value)
  def removeWeakConnections(mat: IndexedRowMatrix): IndexedRowMatrix ={
    new IndexedRowMatrix(
      mat.rows.map{row =>
        val svec = row.vector.toSparse
        IndexedRow(row.index,
          new SparseVector(svec.size, svec.indices,
            svec.values.map(v => {
              if(v < epsilon) 0.0
              else v
            })
          ))
      })
  }

  // TODO Use another object to speed up join between RDD
  def difference(m1: IndexedRowMatrix, m2: IndexedRowMatrix): Double = {
    val m1RDD:RDD[((Long,Int),Double)] = m1.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })
    val m2RDD:RDD[((Long,Int),Double)] = m2.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })
    val diffRDD = m1RDD.join(m2RDD).map(diff => Math.abs(diff._2._1-diff._2._2))
    diffRDD.sum()/diffRDD.count()
  }

  /*
   * Train MCL algorithm.
   */
  def run(graph: Graph[String, Double]): MCLModel = {

    val mat = toIndexedRowMatrix(graph)
    val vertices: VertexRDD[String] = graph.vertices

    // Number of current iterations
    var iter = 0
    // Convergence indicator
    var change = convergenceRate + 1

    //TODO Cache adjacency matrix to improve algorithm perfomance
    var M1:IndexedRowMatrix  = normalization(mat)
    while (iter < maxIterations && change > convergenceRate) {
      val M2: IndexedRowMatrix = removeWeakConnections(normalization(inflation(expansion(M1))))
      change = difference(M1, M2)
      iter = iter + 1
      M1 = M2
    }

    // Method 1 Get Strongly Connected Components and their neighbors to assign each to 1 or more clusters
    /*val randomWalksGraph: Graph[String, Double] = toGraph(M1, vertices)

    val SCCgraph = randomWalksGraph.stronglyConnectedComponents(10)
    val assignmentsSCC: RDD[Assignment] =
      SCCgraph.vertices.map{
        case (id: Long, cluster: Long) => Assignment(id, cluster)
      }

    val NSCCvertices = SCCgraph.collectNeighbors(EdgeDirection.Either)
    val assignmentsNSCC: RDD[Assignment] =
      NSCCvertices.flatMap(
        node => node._2.map{
          case (id: Long, cluster: Long) => Assignment(id, cluster)
        }
      )

    val assignmentsRDD: RDD[Assignment] = assignmentsSCC.union(assignmentsNSCC).distinct()*/

    // Method 2 Get attractors in adjency matrix (nodes with not only null values) and collect every nodes they are attached to in order to form a cluster.

    val assignmentsRDD: RDD[Assignment]=
      M1.rows.flatMap(r => {
        val sv = r.vector.toSparse
        sv.indices.map(i => Assignment(r.index, i))
      })

    new MCLModel(assignmentsRDD)
  }


  //To transform a graph in an IndexedRowMatrix - TODO Add to graphX Graph Class
  def toIndexedRowMatrix(graph: Graph[String, Double]): IndexedRowMatrix = {
    val sc:SparkContext = graph.edges.sparkContext

    //No assumptions about a wrong graph format for the moment.
    //Especially relationships values have to be checked before doing what follows
    val rawEntries: RDD[(Int, (Int, Double))] = graph.edges.map(
      e => (e.srcId.toInt, (e.dstId.toInt, e.attr))
    )

    val numOfNodes:Int =  graph.numVertices.toInt

    //Test whether self loops have already been initialized
    val selfLoopBool:Boolean = rawEntries.filter(e => e._1 == e._2._1 & e._2._2 == 0.0).count == numOfNodes

    val entries: RDD[(Int, (Int, Double))] = selfLoopBool match {
      //Keep current weights that way
      case true => rawEntries

      //Give a weight of one for each edge from one node to itself
      case false =>
        val entriesWithoutSelfLoop: RDD[(Int, (Int, Double))] = rawEntries

        //Add self loop to each node
        val numPartitions: Int = entriesWithoutSelfLoop.partitions.length
        val nodesPerPartitions: Int = math.ceil(numOfNodes.toDouble / numPartitions.toDouble).toInt

        val ran: Range = 0 to numPartitions - 1
        val recordPerPartition: RDD[Range] = {
          sc.parallelize(ran.map(
            i => {
              val maxBound: Int = ((i + 1) * nodesPerPartitions) - 1
              if (maxBound > numOfNodes - 1) {
                i * nodesPerPartitions to numOfNodes - 1
              }
              else {
                i * nodesPerPartitions to maxBound
              }
            })
          )
        }

        val selfLoop: RDD[(Int, (Int, Double))] = {
          recordPerPartition.flatMap(nodes =>
            nodes.map(n =>
              (n, (n, 1.0))
            ))
        }

        entriesWithoutSelfLoop.union(selfLoop)

    }

    val indexedRows = entries.groupByKey().map(e =>
      IndexedRow(e._1, Vectors.sparse(numOfNodes, e._2.toSeq))
    )

    new IndexedRowMatrix(indexedRows, nRows = numOfNodes, nCols = numOfNodes)
  }

  //To transform an IndexedRowMatrix in a graph - TODO Add to mllib IndexedRowMatrix Class
  def toGraph(mat: IndexedRowMatrix, vertices: VertexRDD[String]): Graph[String, Double] = {
    val edges: RDD[Edge[Double]] =
      mat.rows.flatMap(f = row => {
        val svec: SparseVector = row.vector.toSparse
        val it:Range = svec.indices.indices
        it.map(ind => Edge(row.index, svec.indices.apply(ind), svec.values.apply(ind)))
      })
    Graph(vertices, edges)
  }
}

object MCL{

  /*
   * Trains a MCL model using the given set of parameters.
   *
   * @param graph training points stored as `BlockMatrix`
   * @param expansionRate expansion rate of adjacency matrix at each iteration
   * @param inflationRate inflation rate of adjacency matrix at each iteration
   * @param convergenceRate stop condition for convergence of MCL algorithm
   * @param epsilon minimum percentage of a weight edge to be significant
   * @param maxIterations maximal number of iterations for a non convergent algorithm
   */
  def train(graph: Graph[String, Double],
            expansionRate: Int = 2,
            inflationRate: Double = 2.0,
            convergenceRate: Double = 0.01,
            epsilon : Double = 0.01,
            maxIterations: Int = 10): MCLModel = {

    new MCL()
      .setExpansionRate(expansionRate)
      .setInflationRate(inflationRate)
      .setConvergenceRate(convergenceRate)
      .setEpsilon(epsilon)
      .setMaxIterations(maxIterations)
      .run(graph)
  }

}