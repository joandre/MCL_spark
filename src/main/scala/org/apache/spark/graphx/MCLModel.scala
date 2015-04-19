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

package org.apache.spark.graphx

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{Loader, MLUtils, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{Logging, SparkContext, SparkException}

/**
 * Model produced by [[MCLModel]].
 *
 * @param expansionRate expansion rate of adjency matrix at each iteration
 * @param inflationRate inflation rate of adjency matrix at each iteration
 * @param epsilon stop condition for convergence of MCL algorithm
 * @param maxIterations maximal number of iterations for a non convergent algorithm
 * @param assignments an RDD of clustering assignements
 */

class MCLModel(
                private var expansionRate: Double,
                private var inflationRate: Double,
                private var epsilon: Double,
                private var maxIterations: Int,
                private var assignments: RDD[Array[(Int,Int)]]) extends Saveable with Serializable{

  override def save(sc: SparkContext, path: String): Unit = {
    MCLModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}

object MCLModel extends Loader[MCLModel]{

  override def load(sc: SparkContext, path: String): MCLModel = {
    MCLModel.SaveLoadV1_0.load(sc, path)
  }

  private[clustering]
  object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.MCLModel"

    def save(sc: SparkContext, model: MCLModel, path: String): Unit = {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("expansionRate" -> model.expansionRate) ~ ("inflationRate" -> model.inflationRate) ~
          ("epsilon" -> model.epsilon) ~ ("maxIterations" -> model.maxIterations)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val dataRDD = model.assignments.toDF()
      dataRDD.saveAsParquetFile(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): MCLModel = {
      implicit val formats = DefaultFormats

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val expansionRate = (metadata \ "expansionRate").extract[Double]
      val inflationRate = (metadata \ "inflationRate").extract[Double]
      val epsilon = (metadata \ "epsilon").extract[Double]
      val maxIterations = (metadata \ "maxIterations").extract[Int]

      new MCLModel(expansionRate, inflationRate, epsilon, maxIterations)
    }
  }
}

