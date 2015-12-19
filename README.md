# MCL Spark

**License:** [MIT](https://github.com/joandre/MCL_spark/blob/master/LICENSE.txt)

**MCL Spark** is an experimental project which goal is to implement a graph clustering algorithm in [Spark](https://github.com/apache/spark), using especially distributed matrix tools embedded in the scala API.

Why MCL algorithm? Because it responds to Spark MLLib [contribution policy](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark#ContributingtoSpark-MLlib-specificContributionGuidelines) first four points:
 * Be widely known
 * Be used and accepted
 * Be highly scalable
 * Be well documented

Please do not hesitate to post comments or questions.

Most of the following content is based on Stijn van Dongen website (http://micans.org/mcl/).

Table of Contents
=================

* [MCL Spark](#mcl-spark)
    * [Getting Started](#getting-started)
      * [Requirements](#requirements)
      * [Building From Sources](#building-from-sources)
      * [Use embarked example](#use-embarked-example)
      * [Parameters choices and Graph advices](#parameters-choices)
    * [MCL (Markov Cluster) algorithm theory](#mcl-markov-cluster-algorithm-theory)
      * [Expansion](#expansion)
      * [Inflation](#inflation)
      * [Convergence and clusters interpretation](#convergence-and-clusters-interpretation)
      * [Optimizations](#optimizations)
    * [Implementation thoughts](#implementation-thoughts)
      * [Spark matrices universe](#spark-matrices-universe)
        * [IndexedRowMatrix](#indexedrowmatrix)
        * [BlockMatrix](#blockmatrix)

## Getting Started

### Requirements

* JDK 1.7 or higher
* SBT 0.13.9 (see http://www.scala-sbt.org/download.html for more information)
* Tested on Spark 1.5.2

### Building From Sources

This library is built with SBT. To build a JAR file simply run "sbt package" from the project root. Currently project was built under scala 2.10.4.

### Use embarked example

```

$MCL_SPARK_HOME/sbt "run [--expansionRate num] [--inflationRate num] [--convergenceRate num] [--epsilon num] [--maxIterations num]"

```

### Import MCL into your Spark Shell

```

$SPARK_HOME/bin/spark-shell --jars $MCL_SPARK_HOME/target/scala-2.10/mcl_spark_2.10-0.1.0.jar 

```

Then use MCL as follows:

```
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.{Assignment, MCL}
import org.apache.spark.rdd.RDD

val users: RDD[(VertexId, String)] =
            sc.parallelize(Array((0L,"Node1"), (1L,"Node2"),
              (2L,"Node3"), (3L,"Node4"),(4L,"Node5"),
              (5L,"Node6"), (6L,"Node7"), (7L, "Node8")))

// Create an RDD for edges
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

// Build the initial Graph
val graph = Graph(users, relationships)
graph.cache()

val clusters: RDD[Assignment] =
    MCL.train(graph).assignments
clusters
    .map(ass => (ass.cluster, ass.id))
    .groupByKey()
    .foreach(cluster =>
        println(cluster._1 + " => " + cluster._2.map(node => node).toString)
    )

```

### Parameters choices

**Inflation and Expansion rates** => The two parameters influence what we call cluster granularity, so how many and how strong should be detected groups of nodes. Inflation increases intra cluster links and decreases inter cluster links while expansion connects nodes to longer and new parts of the graph. **Default = 2**

1. A big inflation rate will strengthen existing clusters.
2. A big expansion rate will boost clusters merging.

Nota bene: Only integers are accepted for expansion rate for now (for computational reasons).

**Convergence rate** => Depending on how fast you want the algorithm to converge. Higher is the value, faster is MCL converging. **Default = 0.01**

**Epsilon** => It is used to set to zero some negligible values (see Optimization paragraph for more details). **Default = 0.01**
 
**Maximum number of iterations** => Regarding micans recommendations, a steady state is usually reached after 10 iterations (default value of maxIterations). **Default = 10**

## MCL (Markov Cluster) algorithm theory

To detect clusters inside a graph, MCL algorithm uses a Column Stochastic Matrix representation and the concept of random walks. The idea is that random walks between two nodes of a same group are more frequent than between two nodes belonging to different ones. So we should compute probability that a node reach each other node of the graph to have a better insight of clusters.

**Definition**: A Column Stochastic Matrix (CSM) is a non-negative matrix which each column sum is equal to 1. In our case, we will prefer Row Stochastic Matrix (RSM) instead of CSM to use Spark API tools (see Implementation thoughts for more details).

Two steps are needed to simulate random walks on a graph: expansion and inflation. Each step is associated with a specific rate (respectively eR and iR). In the following formula, n is the number of nodes in the graph.

### Expansion
To perform **expansion**, we raise the stochastic matrix to the power eR using the normal matrix product.

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/Expansion.png"/> </p>

, for eR = 2.

### Inflation
To perform **inflation**, we apply the Hadamard power on the RSM (powers entrywise) and we then normalize each row to get back to probabilities.

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/Inflation.png"/> </p>

### Convergence and clusters interpretation

After each loop (expansion and inflation), a convergence test is applied on the new matrix. If the matrix remains stable between two loops (the difference between probabilities of random walks is inferior to a certain convergence rate), then the algorithm stops. Otherwise, a maximum number of iterations is defined to force the process to reach a steady state.

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/Difference.png"/> </p>


Finally we look for weakly connected components to define which cluster(s) a node belongs to. In our case, a weakly connected component is a cluster of strongly connected nodes (every nodes are linked) and all their respective neighbors. A cluster will be a star with one or several attractor(s) in the center (see example below). A node can belong to one or several cluster(s).

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/MCL.png"/> </p>

### Optimizations
Most of the following solutions were developed by Stijn van Dongen. More could come based on matrix distribution state.

 * Add self loop to each node. For now, a neutral weight is imposed. A more important weight would increase cluster granularity. Inflation and expansion rates are still parameterizable to influence that phenomena.
 * Most of big graph are sparse because of their nature. For example, in a social graph, people are not related to every other users but mostly to relatives, friends or colleagues (depending on the nature of the social network). In inflation and expansion steps, "weak" connections between nodes weight tend to zero (since it is the goal to detect strong connections in order to bring out clusters) without reaching it. In order to take advantage of sparsity representation of the graph, this value should be set to zero after each iteration, if it is lower than a very small epsilon (e.g. 0.01).
 * In order to improve convergence test speed, MCL author proposed a more efficient way to proceed. (Not Implemented Yet)

## Implementation thoughts

### Spark matrices universe
As explained in introduction, this program is exclusively based on scala matrices Spark API. Two main matrix types are explored to realize inflation, expansion and normalization step: [IndexedRowMatrix](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix) and [BlockMatrix](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix).

#### IndexedRowMatrix
 * Advantages: Each row can be stored in a sparse way, normalization is easy to apply since we apply it per row (instead of column like in the original implementation).
 * Disadvantages: No multiplication between two IndexedRowMatrix available.

#### BlockMatrix
 * Advantages: Fully scalable => Blocks of adjustable size (1024x1024 by default), with sparse matrices using [Compressed Sparse Column](http://netlib.org/linalg/html_templates/node92.html)
 * Disadvantages: Hard to implement normalization.

The last option available is to transform adjacency matrix from BlockMatrix to IndexedRowMatrix (and vice versa) which can be a very expensive operation for large graph.

