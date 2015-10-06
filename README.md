# MCL Spark

**License:** [MIT](https://github.com/joandre/MCL_spark/blob/master/LICENSE.txt)

**MCL Spark** is an experimental project which goal is to implement a graph clustering algorithm in [Spark](https://github.com/apache/spark), using especially distributed matrix tools embedded in the scala API.

Why MCL algorithm? Because it responds to Spark MLLib [contribution chart](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark#ContributingtoSpark-MLlib-specificContributionGuidelines) first four points:
 * Be widely known
 * Be used and accepted (academic citations and concrete use cases can help justify this)
 * Be highly scalable
 * Be well documented

Please do not hesitate to post comments or questions.

## MCL (Markov Cluster) algorithm theory
Most of this paragraph content is base on Stijn van Dongen website (http://micans.org/mcl/).

To detect clusters inside a graph, MCL algorithm uses a Column Stochastic Matrix representation and the concept of random walks. The idea is that random walks between two nodes of a same group are more frequent than between two nodes belonging to different ones. So we should compute probabilities that each node reach each other nodes in the graph to have a better insight of clusters.

**Definition**: A Column Stochastic Matrix (CSM) is a non-negative matrix which each column sum is equal to 1. In our case, we will prefer Row Stochastic Matrix (RSM) instead of CSM to use Spark API tools (see Implementation thoughts for more details).

Two steps are needed to simulate random walks on a graph: expansion and inflation. Each step is associated with a specific rate (respectively eR and iR)

### Expansion
To perform **expansion**, we raise the stochastic matrix to using the normal matrix product.

<div style="text-align:center"><img src ="https://github.com/joandre/MCL_spark/images/Expansion.png" /></div>
<!-- ![Expansion formula](/home/andrejoan/workspace/MCL_spark/images/Expansion.png) -->

See https://en.wikipedia.org/wiki/Exponentiation_by_squaring for an improvement.

### Inflation
To perform **inflation**, we apply the Hadamard power on the RSM (powers entrywise) and we then normalize each row to get back to probabilities.

<div style="text-align:center"><img src ="https://github.com/joandre/MCL_spark/images/Inflation.png" /></div>
<!-- ![Inflation formula](/home/andrejoan/workspace/MCL_spark/images/Inflation.png) -->

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

### Tricks and smart solutions
Most of the following solutions were developed by Stijn van Dongen. More could come based on matrix distribution state.

 * Add self loop to each node (See: https://www.cs.ucsb.edu/~xyan/classes/CS595D-2009winter/MCL_Presentation2.pdf)
 * Most of big graph are sparse because of their nature. For example, in a social graph, people are not related to every other users but mostly to relatives, friends or colleagues (depending on the nature of the social network). In inflation and expansion steps, "weak" connections between nodes weight tend to zero (since it is the goal to detect strong connections in order to bring out clusters) without reaching it. In order to take advantage of sparsity representation of the graph, this value should be set to zero after each iteration, if it is lower than a very small epsilon (e.g. 0.01). (Not Implemented Yet)
 * In order to improve convergence test speed, MCL author proposed a more efficient way to proceed. (Not Implemented Yet)


