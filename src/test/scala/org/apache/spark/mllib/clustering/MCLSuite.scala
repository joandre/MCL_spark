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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

class MCLSuite extends MCLFunSuite{
  // Disable Spark messages when running programm
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  /*test("Matrix Normalization") {

    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    // Load data
    val matrix:Seq[String] = Source.fromURL(getClass.getResource("/OrientedMatrixSelfLoop.txt")).getLines().toSeq
    val matrixNormalized:Seq[String] = Source.fromURL(getClass.getResource("/OrientedNormalizedMatrix.txt")).getLines().toSeq

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
                    line.split(",").map(e => e.toDouble)
                  )
                )
            }
        )
      )

    var range2:Long = 0
    val initialNormalizedMatrix =
      new IndexedRowMatrix(
        sc.parallelize(
          matrixNormalized
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

    val adjacencyNormalizedMat: IndexedRowMatrix =
      new IndexedRowMatrix(
      initialMatrix.rows
        .map{row =>
          val svec = row.vector.toSparse
          IndexedRow(row.index,
            new SparseVector(
              svec.size,
              svec.indices,
              svec.values.map(v =>
                BigDecimal(v/svec.values.sum).setScale(10, BigDecimal.RoundingMode.HALF_UP).toDouble)))
        })

    initialNormalizedMatrix.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      .join(
        adjacencyNormalizedMat.rows.map(iRow => (iRow.index, iRow.vector.toArray))
      )
      .collect.foreach(pairOfRows => pairOfRows._2._1 shouldEqual pairOfRows._2._2)

    sc.stop()
  }

  test("Matrix Expansion") {

  }

  test("Matrix Inflation") {

  }

  test("Remove Weak Connections") {

  }

  test("Difference Between Two Matrices") {

  }*/

  test("Official MCL Algorithm Versus Spark MCL") {

    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    /*val relationships: RDD[Edge[Double]] =
      sc.textFile("/Data/Oriented_dataset/edges.csv")
        .map(line => line.split(","))
        .map(e => Edge(e(0).toLong, e(1).toLong, e(2).toDouble))

    val users: RDD[(VertexId, String)] =
      sc.textFile("/Data/Oriented_dataset/nodes.txt")
        .map(line => line.split(","))
        .map(n => (n(0).toLong, n(1)))

    val graph: Graph[String, Double] = Graph(users, relationships)*/

    val relationshipsFile:Seq[String] = Source.fromURL(getClass.getResource("/MCL/graph.tab")).getLines().toSeq
    val clustersFile:Seq[String] = Source.fromURL(getClass.getResource("/MCL/clusters")).getLines().toSeq

    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        relationshipsFile
        .map(line => line.split(" "))
        .map(e => Edge(e(0).toLong, e(1).toLong, e(2).toDouble))
      )

    val graph: Graph[String, Double] = Graph.fromEdges(relationships, "default")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val clusters =
      MCL.train(graph, convergenceRate = 0.01, epsilon=0.01, maxIterations=50, selfLoopWeight = 1.0, graphOrientationStrategyOption = "bidirected").assignments
        .map(assignment => (assignment.cluster, assignment.id))
        .groupByKey()
        .map(cluster => (cluster._1, cluster._2.toArray))
        .toDF("clusterId", "cluster")

    /*val clustersChallenge =
      sc.textFile("/Data/Oriented_dataset/clusters")
          .map(line => line.split("\t").map(node => node.toLong).toList)
          .map(assignment => (assignment.max, assignment.toArray))
          .toDF("clusterId", "cluster")*/

    val clustersChallenge =
      sc.parallelize(
        clustersFile
        .map(line => line.split("\t").map(node => node.toLong).toList)
        .map(assignment => (assignment.max, assignment.toArray))
      ).toDF("clusterId", "cluster")

    clustersChallenge.foreach(println)

    println("number of real clusters: " + clustersChallenge.count)
    println("number of nodes in the graph: " + graph.vertices.count)
    println("number of cluster obtained by MCL: " + clusters.count)

    /*clusters
      .sortBy(a => a._2.size)
      .foreach(cluster =>
        println(cluster._1 + " => " + cluster._2.map(node => node).toString)
      )*/

    val test = clusters.join(clustersChallenge, clusters.col("cluster")===clustersChallenge.col("cluster"))
    test.count shouldEqual clustersChallenge.count

    sc.stop()
  }


}
