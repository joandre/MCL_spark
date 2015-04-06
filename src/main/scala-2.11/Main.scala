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

// Import required spark classes
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.MCL

// Define main method (scala entry point)
object Main {
  def main(args: Array[String]) {

    // Initialise spark context
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MCL")
      .set("spark.executor.memory","1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction","1")

    val sc = new SparkContext(conf)

    // Create and RDD for vertices
    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array((1L,"Node1"), (2L,"Node2"),
        (3L,"Node3"), (4L,"Node4"),(5L,"Node5"),
        (6L,"Node6"), (7L,"Node7")))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(
        Array(Edge(1, 2, "linkedto"), Edge(1, 3, "linkedto"), Edge(1, 4, "linkedto"),
        Edge(2, 3, "linkedto"), Edge(2, 4, "linkedto"), Edge(2, 5, "linkedto"),
        Edge(3, 4, "linkedto"), Edge(5, 6, "linkedto"), Edge(2, 7, "linkedto"),
        Edge(6, 7, "linkedto")))

    // Build the initial Graph
    val graph = Graph(users, relationships)
    graph.cache()
    graph.edges.foreach(println)

    println("Hello, world!")
    // Terminate spark context
    sc.stop()
  }
}