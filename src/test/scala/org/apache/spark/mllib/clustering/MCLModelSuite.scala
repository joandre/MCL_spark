package org.apache.spark.mllib.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.util.Utils

/**
  * Created by andrejoan on 4/30/16.
  */
class MCLModelSuite extends MCLFunSuite{
  // Disable Spark messages when running program
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  test("model save/load", UnitTest){

    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array((0L,"Node1"), (1L,"Node2"),
        (2L,"Node3"), (3L,"Node4"),(4L,"Node5"),
        (5L,"Node6"), (6L,"Node7"), (7L, "Node8"),
        (8L, "Node9"), (9L, "Node10"), (10L, "Node11")))

    val relationships: RDD[Edge[Double]] =
      sc.parallelize(
        Seq(Edge(0, 1, 1.0), Edge(1, 0, 1.0),
          Edge(0, 2, 1.0), Edge(2, 0, 1.0),
          Edge(0, 3, 1.0), Edge(3, 0, 1.0),
          Edge(1, 2, 1.0), Edge(2, 1, 1.0),
          Edge(1, 3, 1.0), Edge(3, 1, 1.0),
          Edge(2, 3, 1.0), Edge(3, 2, 1.0),
          Edge(4, 5, 1.0), Edge(5, 4, 1.0),
          Edge(4, 6, 1.0), Edge(6, 4, 1.0),
          Edge(4, 7, 1.0), Edge(7, 4, 1.0),
          Edge(5, 6, 1.0), Edge(6, 5, 1.0),
          Edge(5, 7, 1.0), Edge(7, 5, 1.0),
          Edge(6, 7, 1.0), Edge(7, 6, 1.0),
          Edge(3, 8, 1.0), Edge(8, 3, 1.0),
          Edge(9, 8, 1.0), Edge(8, 9, 1.0),
          Edge(9, 10, 1.0), Edge(10, 9, 1.0),
          Edge(4, 10, 1.0), Edge(10, 4, 1.0)
        ))

    val graph = Graph(users, relationships)

    val model: MCLModel = MCL.train(graph)

    // Check number of clusters
    model.nbClusters shouldEqual 3

    // Check save and load methods
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Array(true, false).foreach { case selector =>
      // Save model, load it back, and compare.
      try {
        model.save(sc, path)
        val sameModel = MCLModel.load(sc, path)
        assertDatasetEquals(model.assignments.orderBy("id"), sameModel.assignments.orderBy("id"))
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }

  }

  test("nodes assignments", UnitTest) {
    val nodeId = 1.0.toLong
    val cluster = 2.0.toLong
    val newAssignment:Assignment = Assignment.apply(Row(nodeId, cluster))

    newAssignment.id shouldEqual nodeId
    newAssignment.cluster shouldEqual cluster
  }

}
