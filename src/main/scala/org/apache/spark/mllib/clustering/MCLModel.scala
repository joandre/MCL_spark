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
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/*
 * A clustering model for MCL.
 *
 * @param expansionRate expansion rate of adjacency matrix at each iteration
 * @param inflationRate inflation rate of adjacency matrix at each iteration
 * @param epsilon stop condition for convergence of MCL algorithm
 * @param maxIterations maximal number of iterations for a non convergent algorithm
 * @param assignments an RDD of clustering assignements
 */

class MCLModel(var assignments: RDD[Assignment]) extends Saveable with Serializable{

  // Number of clusters.
  def nbClusters: Int = assignments.count().toInt

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
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)
        /* ~ ("expansionRate" -> model.expansionRate) ~ ("inflationRate" -> model.inflationRate)
         ~ ("epsilon" -> model.epsilon) ~ ("maxIterations" -> model.maxIterations) */))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val dataRDD = model.assignments.toDF()
      dataRDD.write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): MCLModel = {
      implicit val formats = DefaultFormats
      val sqlContext = new SQLContext(sc)

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      /*val expansionRate = (metadata \ "expansionRate").extract[Double]
      val inflationRate = (metadata \ "inflationRate").extract[Double]
      val epsilon = (metadata \ "epsilon").extract[Double]
      val maxIterations = (metadata \ "maxIterations").extract[Int]*/

      val assignments = sqlContext.read.parquet(Loader.dataPath(path))
      // Check if loading file respects Assignment class schema
      Loader.checkSchema[Assignment](assignments.schema)
      val assignmentsRDD = assignments.toDF().map {
        case Row(id: Long, cluster: Long) => Assignment(id, cluster)
      }

      new MCLModel(assignmentsRDD)
    }
  }
}

/*
* List which point belongs to which cluster
*/

case class Assignment(id: Long, cluster: Long)

private object Assignment {
  def apply(r: Row): Assignment = {
    Assignment(r.getLong(0), r.getLong(1))
  }
}