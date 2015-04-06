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

package org.apache.spark.graphx.GraphMatrix

import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

abstract class GraphMatrix() extends Graph{

  //To transform a graph in a coordinate matrix
  def toCoordinateMatrix(graph: Graph): CoordinateMatrix = {
    //No assumptions about a wrong graph format for the moment.
    //Especially reelationships values have to be checked before doing what follows
    val entries: RDD[MatrixEntry] = graph.mapEdges(e => (e._1,e._2,e._3))
    val mat: CoordinateMatrix = new CoordinateMatrix(entries)

    val m = mat.numRows()
    val n = mat.numCols()
    println("\n" + m + "\n" + n)
    mat
  }
}
