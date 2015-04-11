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

import org.apache.spark.mllib.linalg.distributed.BlockMatrix

//Why classes are private in spark project ?

class MCL private(
                   private var expansionRate: Double,
                   private var inflationRate: Double,
                   private var epsilon: Double,
                   private var maxIterations: Int) extends Serializable{

  /**
   * Constructs a MCL instance with default parameters: {expansionRate: 2, inflationRate: 2,
   * epsilon: 0.01, maxIterations: 10}.
   */
  def this() = this(2, 2, 0.01,10)

  /**
   * Expansion rate ...
   */
  def getExpansionRate: Double = expansionRate

  /**
   * Set the expansion rate. Default: 2.
   */
  def setExpansionRate(expansionRate: Double): this.type = {
    this.expansionRate = expansionRate
    this
  }

  /**
   * Inflation rate ...
   */
  def getInflationRate: Double = inflationRate

  /**
   * Set the inflation rate. Default: 2.
   */
  def setInflationRate(inflationRate: Double): this.type = {
    this.inflationRate = inflationRate
    this
  }

  /**
   * Stop condition for convergence of MCL algorithm
   */
  def getEpsilon: Double = epsilon

  /**
   * Set the convergence condition. Default: 0.01.
   */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  /**
   * Stop condition if MCL algorithm does not converge fairly quickly
   */
  def getEMaxIterations: Double = maxIterations

  /**
   * Set maximum number of iterations. Default: 10.
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Train MCL algorithm.
   */
  def run(data: BlockMatrix): Array[(Int,String)] = {
    data.multiply(data.transpose)
    println(data.toString)
    Array(null)
  }

}

object MCL{

  /**
   * Trains a MCL model using the given set of parameters.
   *
   * @param data training points stored as `BlockMatrix`
   * @param expansionRate expansion rate of adjency matrix at each iteration
   * @param inflationRate inflation rate of adjency matrix at each iteration
   * @param epsilon stop condition for convergence of MCL algorithm
   * @param maxIterations maximal number of iterations for a non convergent algorithm
   */
  def train(
             data: BlockMatrix,
             expansionRate: Double,
             inflationRate: Double,
             epsilon: Double,
             maxIterations: Int): Array[(Int,String)] = { //MCLModel ?
    new MCL().setExpansionRate(expansionRate)
      .setInflationRate(inflationRate)
      .setEpsilon(epsilon)
      .setMaxIterations(maxIterations)
      .run(data)
  }

  /**
   * Trains a MCL model using the given set of parameters.
   *
   * @param data training points stored as `BlockMatrix`
   */
  def train(data: BlockMatrix): Array[(Int,String)] = { //MCLModel ?
    new MCL().run(data)
  }

}