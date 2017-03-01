[![Build Status](https://travis-ci.org/joandre/MCL_spark.svg?branch=master)](https://travis-ci.org/joandre/MCL_spark)
[![codecov](https://codecov.io/gh/joandre/MCL_spark/branch/master/graph/badge.svg)](https://codecov.io/gh/joandre/MCL_spark)

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
      * [Online Documentation](#online-documentation)
      * [Requirements](#requirements)
      * [Building From Sources](#building-from-sources)
      * [Use embarked example](#use-embarked-example)
      * [Parameters choices](#parameters-choices)
    * [MCL (Markov Cluster) algorithm theory](#mcl-markov-cluster-algorithm-theory)
      * [Expansion](#expansion)
      * [Inflation](#inflation)
      * [Convergence and clusters interpretation](#convergence-and-clusters-interpretation)
      * [Optimizations](#optimizations)
    * [Implementation thoughts](#implementation-thoughts)
      * [Spark matrices universe](#spark-matrices-universe)
        * [IndexedRowMatrix](#indexedrowmatrix)
        * [BlockMatrix](#blockmatrix)
      * [Directed graph management](#directed-graph-management)
      * [Hypergraph](#hypergraph)
    * [References](#references)

## Getting Started

### Online Documentation

A Scaladoc is available [here](http://joandre.github.io/docs/MCL_Spark/api/).

### Requirements

* JDK 1.8 or higher
* SBT 0.13.9 (see http://www.scala-sbt.org/download.html for more information)
* Build against Spark 1.6.1+

### Building From Sources

This library is built with SBT. To build a JAR file simply run "sbt package" from the project root. Currently project was built under scala 2.10.5.

### Use embarked example

```

$MCL_SPARK_HOME/sbt "run [--expansionRate num] [--inflationRate num] [--epsilon num] [--maxIterations num]  [--selfLoopWeight num] [--graphOrientationStrategy string]"

```

### Import MCL into your Spark Shell

```

$SPARK_HOME/bin/spark-shell --jars $MCL_SPARK_HOME/target/scala-2.11/mcl_spark_2.11-1.0.0.jar 

```

Then use MCL as follows:

```
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.{Assignment, MCL}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{sort_array,collect_list,col}

val users: RDD[(VertexId, String)] =
            sc.parallelize(Array((0L,"Node1"), (1L,"Node2"),
                (2L,"Node3"), (3L,"Node4"),(4L,"Node5"),
                (5L,"Node6"), (6L,"Node7"), (7L, "Node8"),
                (8L, "Node9"), (9L, "Node10"), (10L, "Node11")))

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

val clusters: Dataset[Assignment] =
    MCL.train(graph).assignments
clusters
    .groupBy("cluster")
    .agg(sort_array(collect_list(col("id"))))
    .show(3)

```

### Parameters choices

**Inflation and Expansion rates** => The two parameters influence what we call cluster granularity, so how many and how strong should be detected groups of nodes. Inflation increases intra cluster links and decreases inter cluster links while expansion connects nodes to further and new parts of the graph. **Default = 2**

1. A big inflation rate will strengthen existing clusters.
2. A big expansion rate will boost clusters merging.

Nota bene: Only integers are accepted for expansion rate for now (for computational reasons).

**Epsilon** => In order to keep the adjacency matrix associated with our graph sparse, one strategy is to remove some negligible edges regarding its weight. Let's say you chose an epsilon equal to 0.05. This means that every edge, connected to one node, which weight is inferior to 5% of the sum of every edges weight connected to our node is removed (see Optimization paragraph for more details). **Default = 0.01**
 
**Maximum number of iterations** => It forces MCL to stop before it converges. Regarding Stijn van Dongen recommendations, a steady state is usually reached after 10 iterations. **Default = 10**

**Self loops weight management** => A percentage of the maximum weight can be applied to added self loops. For example, for a binary graph, 1 is the maximum weight to allocate (see Optimization paragraph for more details). **Default = 0.1**

**Directed and undirected graphs management** => To deal with directed graphs. **Default = "undirected"**

1. "undirected": graph is supposed undirected. No edges are added.
2. "directed": graph is supposed directed. Each edge inverse is added so graph becomes undirected.
3. "bidirected": graph already owns bidirected edges. Excepted for already existing undirected edges, each edge inverse is added so graph becomes undirected.

See [Implementation thoughts](#implementation-thoughts) for more details.

## MCL (Markov Cluster) algorithm theory

### Recall about Markov chains

*"A Markov chain is a sequence of random variables X1, X2, X3, ... with the Markov property, namely that the probability of moving to next state depends only on the present state and not on the previous states."* ([wikipedia definition](https://en.wikipedia.org/wiki/Markov_chain#Formal_definition))

**Defintion**: a state is absorbent when it cannot be left.

**Definition**: a Markov chain is aperiodic, if it at least one of its state has a period of 1, so returning to the original state occurs irregularly.

**Definition**: a Markov chain is irreducible, if it is possible to get to any state from any state.

**Definition**: a Markov chain is ergodic, if it is both aperiodic and irreducible.

### Principle

To detect clusters inside a graph, MCL algorithm uses a Column Stochastic Matrix representation and the concept of random walks. The idea is that random walks between two nodes that belong to the same group are more frequent than between two nodes belonging to different groups. So we should compute probability that a node reach each other node of the graph to have a better insight of clusters.

**Definition**: a Column Stochastic Matrix (CSM) is a non-negative matrix which each column sum is equal to 1. In our case, we will prefer Row Stochastic Matrix (RSM) instead of CSM to use Spark API tools (see Implementation thoughts for more details).

Two steps are needed to simulate random walks on a graph: expansion and inflation. Each step is associated with a specific rate (respectively eR and iR). In the following formula, n is the number of nodes in the graph.

### Expansion
To perform **expansion**, we raise the stochastic matrix to the power eR using the normal matrix product.

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/Expansion.png"/> </p>

, for eR = 2.

### Inflation
To perform **inflation**, we apply the Hadamard power on the RSM (powers entrywise) and we then normalize each row to get back to probabilities.

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/Inflation.png"/> </p>

### Convergence and clusters interpretation

After each loop (expansion and inflation), a convergence test is applied on the new matrix. When it remains stable regarding the previous iteration, then the algorithm stops. Otherwise, a maximum number of iterations is defined to force the process to reach a steady state.

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/Difference.png"/> </p>

Each non-empty column (with non-zero values) of A, corresponds to a cluster and its composition. A cluster will be a star with one or several attractor(s) in the center (see example below).

<p align="center"> <img src="https://github.com/joandre/MCL_spark/blob/master/images/MCL.png" alt="Graph shape for different convergence status (http://micans.org)"/> </p>

A node can belong to one or several cluster(s).

### Optimizations
Most of the following solutions were developed by Stijn van Dongen. More could come based on matrix distribution state.

 * Add self loop to each node. This is generally used to satisfy aperiodic condition of graph Markov chain. More than an optimization, this is required to avoid the non-convergence of MCL because of the infinite alternation between different states (depending on the period). Default weight allocated is the maximum weight of every edges related to the current node. To stay as closed as possible of the true graph, self loop weights can be decreased.
 * Most of big graphs are sparse because of their nature. For example, in a social graph, people are not related to every other users but mostly to relatives, friends or colleagues (depending on the nature of the social network). In inflation and expansion steps, "weak" connections weight tends to zero (since it is the goal to detect strong connections in order to bring out clusters) without reaching it. In order to keep the graph sparse, we can adopt three strategies:
    1. Set every small values to zero regarding a threshold. This can be dangerous when a large percentage of global weight belongs to small edges. Currently, this is the only strategy available.
    2. Keep k largest values for each node. Can be very expensive for very large k and a high number of nonzero entries.
    3. Mix the two strategies so a threshold pruning is first applied to reduce exact pruning cost.
 * In order to improve convergence test speed, MCL author proposed a more efficient way to proceed. (Not Implemented Yet)

## Implementation thoughts

### Spark matrices universe
As explained in introduction, this program is exclusively based on scala matrices Spark API. Two main matrix types are explored to implement inflation, expansion and normalization steps: [IndexedRowMatrix](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix) and [BlockMatrix](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix).

#### IndexedRowMatrix
 * Advantages: Each row can be stored in a sparse way, normalization is easy to apply since we apply it per row (instead of column like in the original implementation).
 * Disadvantages: No multiplication between two IndexedRowMatrix available.

#### BlockMatrix
 * Advantages: Fully scalable => Blocks of adjustable size (1024x1024 by default), with sparse matrices using [Compressed Sparse Column](http://netlib.org/linalg/html_templates/node92.html)
 * Disadvantages: Hard to implement normalization.

For inflation and normalization, adjacency matrix is transformed in IndexedRowMatrix, so computations are done locally.
For expansion, adjacency matrix is transformed in BlockMatrix, so we take advantage of a fully distributed matrix multiplication.
 
### Directed graphs management
To respect the irreducibility of graphs markov chain, MCL is only applied on undirected ones. For example, in a directed bipartite graph, there are a bunch of absorbent states, so associated markov chain is reducible and does not respect ergodic condition.

To offer the possibility to users to apply MCL on directed graphs, the only way is to make the graph symmetric by adding each edge inverse. This is due to GraphX API where edges are only directed. For the particular case of bidirected graphs (where some edges and their inverse already exist), birected edges remain as it is.

Note that symmetry (same weight for an edge and its inverse) is preferred for more efficiency.

### Hypergraph
When two nodes are related to each other with several edges, those edges are merged and their weights summed so there remains only one.

## References

* Stijn van Dongen. MCL - a cluster algorithm for graphs. [Official Website](http://micans.org/mcl/)
* Kathy Macropol. Clustering on Graphs: The Markov Cluster Algorithm (MCL). [A Presentation](https://www.cs.ucsb.edu/~xyan/classes/CS595D-2009winter/MCL_Presentation2.pdf)
* Jean-Benoist Leger, Corinne Vacher, Jean-Jacques Daudin. Detection of structurally homogeneous subsets in graphs. [A Survey](http://vacher.corinne.free.fr/pdf/Leger_StatsComputing_2013.pdf)
