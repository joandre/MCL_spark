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
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.io._

/** Scala Tests class for MCL algorithm */
class MCLSuite extends MCLFunSuite{
  // Disable Spark messages when running programm
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Unit Tests

  test("Matrix Normalization", UnitTest) {

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

    val MCLObject: MCL = new MCL()
    val normalizedMatrix: IndexedRowMatrix = MCLObject.normalization(indexedMatrix)

    val objective: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.5,0,0,0,0.5,0))),
            IndexedRow(1, new DenseVector(Array(0,0.25,0.25,0,0.25,0.25))),
            IndexedRow(2, new DenseVector(Array(0,0.3333333333333333,0.3333333333333333,0,0,0.3333333333333333))),
            IndexedRow(3, new DenseVector(Array(0,0,0,0.5,0,0.5))),
            IndexedRow(4, new DenseVector(Array(0.3333333333333333,0.3333333333333333,0,0,0.3333333333333333,0))),
            IndexedRow(5, new DenseVector(Array(0,0.25,0.25,0.25,0,0.25)))
          )
        ))

    normalizedMatrix.numRows shouldEqual objective.numRows
    normalizedMatrix.numCols shouldEqual objective.numCols
    objective.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      .join(
        normalizedMatrix.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      )
      .collect.foreach(
      pairOfRows =>
      {
        pairOfRows._2._1 shouldEqual pairOfRows._2._2
      }
    )

  }

  test("Matrix Expansion", UnitTest) {

    val normalizedMatrix: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.5,0,0,0,0.5,0))),
            IndexedRow(1, new DenseVector(Array(0,0.25,0.25,0,0.25,0.25))),
            IndexedRow(2, new DenseVector(Array(0,0.33,0.33,0,0,0.33))),
            IndexedRow(3, new DenseVector(Array(0,0,0,0.5,0,0.5))),
            IndexedRow(4, new DenseVector(Array(0.33,0.33,0,0,0.33,0))),
            IndexedRow(5, new DenseVector(Array(0,0.25,0.25,0.25,0,0.25)))
          )
        ))

    val MCLObject: MCL = new MCL()
    val expandedMatrix: IndexedRowMatrix = MCLObject.expansion(normalizedMatrix).toIndexedRowMatrix()

    val objective: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.4150,0.1650,0,0,0.4150,0))),
            IndexedRow(1, new DenseVector(Array(0.0825,0.2900,0.2075,0.0625,0.1450,0.2075))),
            IndexedRow(2, new DenseVector(Array(0,0.2739,0.2739,0.0825,0.0825,0.2739))),
            IndexedRow(3, new DenseVector(Array(0,0.1250,0.1250,0.3750,0,0.3750))),
            IndexedRow(4, new DenseVector(Array(0.2739,0.1914,0.0825,0,0.3564,0.0825))),
            IndexedRow(5, new DenseVector(Array(0,0.2075,0.2075,0.1875,0.0625,0.3325)))
          )
        ))

    expandedMatrix.numRows shouldEqual objective.numRows
    expandedMatrix.numCols shouldEqual objective.numCols
    objective.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      .join(
        expandedMatrix.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      )
      .collect.sortBy(row => row._1).foreach(
      pairOfRows =>
      {
        val expandedRows = pairOfRows._2._2.map(e => BigDecimal(e).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
        pairOfRows._2._1 shouldEqual expandedRows
      }
    )

  }

  test("Matrix Inflation", UnitTest) {

    val expandedMatrix: BlockMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.4150,0.1650,0,0,0.4150,0))),
            IndexedRow(1, new DenseVector(Array(0.0825,0.2900,0.2075,0.0625,0.1450,0.2075))),
            IndexedRow(2, new DenseVector(Array(0,0.2739,0.2739,0.0825,0.0825,0.2739))),
            IndexedRow(3, new DenseVector(Array(0,0.1250,0.1250,0.3750,0,0.3750))),
            IndexedRow(4, new DenseVector(Array(0.2739,0.1914,0.0825,0,0.3564,0.0825))),
            IndexedRow(5, new DenseVector(Array(0,0.2075,0.2075,0.1875,0.0625,0.3325)))
          )
        )).toBlockMatrix

    val MCLObject: MCL = new MCL()
    val inflatedMatrix: IndexedRowMatrix = MCLObject.inflation(expandedMatrix)

    val objective: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.172225,0.027225,0,0,0.172225,0))),
            IndexedRow(1, new DenseVector(Array(0.00680625,0.0841,0.04305625,0.00390625,0.021025,0.04305625))),
            IndexedRow(2, new DenseVector(Array(0,0.07502121,0.07502121,0.00680625,0.00680625,0.07502121))),
            IndexedRow(3, new DenseVector(Array(0,0.015625,0.015625,0.140625,0,0.140625))),
            IndexedRow(4, new DenseVector(Array(0.07502121,0.03663396,0.00680625,0,0.12702096,0.00680625))),
            IndexedRow(5, new DenseVector(Array(0,0.04305625,0.04305625,0.03515625,0.00390625,0.11055625)))
          )
        ))

    inflatedMatrix.numRows shouldEqual objective.numRows
    inflatedMatrix.numCols shouldEqual objective.numCols
    objective.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      .join(
        inflatedMatrix.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      )
      .collect.sortBy(row => row._1).foreach(
      pairOfRows =>
      {
        val inflatedRows = pairOfRows._2._2.map(e => BigDecimal(e).setScale(8, BigDecimal.RoundingMode.HALF_UP).toDouble)
        pairOfRows._2._1 shouldEqual inflatedRows
      }
    )

  }

  test("Remove Weak Connections", UnitTest) {

    val inflatedMatrix: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.172225,0.027225,0,0,0.172225,0))),
            IndexedRow(1, new DenseVector(Array(0.00680625,0.0841,0.04305625,0.00390625,0.021025,0.04305625))),
            IndexedRow(2, new DenseVector(Array(0,0.07502121,0.07502121,0.00680625,0.00680625,0.07502121))),
            IndexedRow(3, new DenseVector(Array(0,0.015625,0.015625,0.140625,0,0.140625))),
            IndexedRow(4, new DenseVector(Array(0.07502121,0.03663396,0.00680625,0,0.12702096,0.00680625))),
            IndexedRow(5, new DenseVector(Array(0,0.04305625,0.04305625,0.03515625,0.00390625,0.11055625)))
          )
        ))

    val MCLObject: MCL = new MCL().setEpsilon(0.01)
    val sparsedMatrix: IndexedRowMatrix = MCLObject.removeWeakConnections(inflatedMatrix)

    val objective: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.172225,0.027225,0,0,0.172225,0))),
            IndexedRow(1, new DenseVector(Array(0,0.0841,0.04305625,0,0.021025,0.04305625))),
            IndexedRow(2, new DenseVector(Array(0,0.07502121,0.07502121,0,0,0.07502121))),
            IndexedRow(3, new DenseVector(Array(0,0.015625,0.015625,0.140625,0,0.140625))),
            IndexedRow(4, new DenseVector(Array(0.07502121,0.03663396,0,0,0.12702096,0))),
            IndexedRow(5, new DenseVector(Array(0,0.04305625,0.04305625,0.03515625,0,0.11055625)))
          )
        ))

    sparsedMatrix.numRows shouldEqual objective.numRows
    sparsedMatrix.numCols shouldEqual objective.numCols
    objective.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      .join(
        sparsedMatrix.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      )
      .collect.sortBy(row => row._1).foreach(
      pairOfRows =>
      {
        val sparsedRows = pairOfRows._2._2.map(e => BigDecimal(e).setScale(8, BigDecimal.RoundingMode.HALF_UP).toDouble)
        pairOfRows._2._1 shouldEqual sparsedRows
      }
    )

  }

  test("Difference Between Two Matrices", UnitTest) {

    val startMatrix: IndexedRowMatrix =
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

    val stopMatrix: IndexedRowMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          Seq(
            IndexedRow(0, new DenseVector(Array(0.172225,0.027225,0,0,0.172225,0))),
            IndexedRow(1, new DenseVector(Array(0,0.0841,0.04305625,0,0.021025,0.04305625))),
            IndexedRow(2, new DenseVector(Array(0,0.07502121,0.07502121,0,0,0.07502121))),
            IndexedRow(3, new DenseVector(Array(0,0.015625,0.015625,0.140625,0,0.140625))),
            IndexedRow(4, new DenseVector(Array(0.07502121,0.03663396,0,0,0.12702096,0))),
            IndexedRow(5, new DenseVector(Array(0,0.04305625,0.04305625,0.03515625,0,0.11055625)))
          )
        ))

    val MCLObject: MCL = new MCL()
    val diff: Double = MCLObject.difference(startMatrix, stopMatrix)

    BigDecimal(diff).setScale(7, BigDecimal.RoundingMode.HALF_UP).toDouble shouldEqual 0.7211179

  }

  // Integration Tests

  test("Official MCL Algorithm Versus Spark MCL", IntegrationTest) {

    val relationshipsFile:Seq[String] = Source.fromURL(getClass.getResource("/MCLUtils/OrientedEdges.txt")).getLines().toSeq
    val clustersFile:Seq[String] = Source.fromURL(getClass.getResource("/MCL/clustersTest")).getLines().toSeq

    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        relationshipsFile
        .map(line => line.split(" "))
        .map(e => Edge(e(0).toLong, e(1).toLong, e(2).toDouble))
      )

    val graph: Graph[String, Double] = Graph.fromEdges(relationships, "default")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val assignments = MCL.train(graph, convergenceRate = 0.01, epsilon=0.05, maxIterations=30, selfLoopWeight = 1.0, graphOrientationStrategy = "bidirected").assignments
    val clusters =
      assignments
        .map(assignment => (assignment.cluster, assignment.id))
        .groupByKey()
        .map(cluster => (1, cluster._2.toArray.sorted))
        .toDF("clusterIdAlgo","cluster")
        .distinct()

    val clustersChallenge =
      sc.parallelize(
        clustersFile
        .map(line => line.split("\t").map(node => node.toLong).toList)
        .map(assignment => (assignment.max, assignment.toArray.sorted))
      ).toDF("clusterIdReal", "cluster")

    val test = clusters.join(clustersChallenge, clusters.col("cluster")===clustersChallenge.col("cluster"))
    val test2 = clusters.join(clustersChallenge, clusters.col("cluster")===clustersChallenge.col("cluster"), "outer")
    test.count shouldEqual clustersChallenge.count

  }


}
