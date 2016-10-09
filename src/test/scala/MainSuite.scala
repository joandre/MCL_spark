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

import org.scalatest.{FunSuite, Matchers}

class MainSuite extends FunSuite with Matchers{

  test("toInt"){
    val eR = Main.toInt(Symbol("expansionRate"), "2")
    eR shouldEqual 2

    an [Exception] should be thrownBy Main.toInt(Symbol("expansionRate"), "1.1")
  }

  test("toDouble"){
    val iR = Main.toDouble(Symbol("inflationRate"), "2.0")
    iR shouldEqual 2.0

    an [Exception] should be thrownBy Main.toDouble(Symbol("inflationRate"), "test")
  }

  test("nextOption"){
    val args: Array[String] = Array("--expansionRate", "3", "--inflationRate", "3.0", "--epsilon", "0.1", "--maxIterations", "20", "--selfLoopWeight", "0.1", "--graphOrientationStrategy", "directed")
    val arglist = args.toList

    val options = Main.nextOption(Map(),arglist)
    Main.toInt('expansionRate, options.getOrElse('expansionRate, 2).toString) shouldEqual 3
    Main.toDouble('inflationRate, options.getOrElse('inflationRate, 2.0).toString) shouldEqual 3.0
    Main.toDouble('epsilon, options.getOrElse('epsilon, 0.01).toString) shouldEqual 0.1
    Main.toInt('maxIterations, options.getOrElse('maxIterations, 10).toString) shouldEqual 20
    Main.toDouble('selfLoopWeight, options.getOrElse('selfLoopWeight, 1.0).toString) shouldEqual 0.1
    options.getOrElse('graphOrientationStrategy, "undirected").toString shouldEqual "directed"

    val args2: Array[String] = Array("--wrongOption", "test")
    val arglist2 = args2.toList

    an [Exception] should be thrownBy Main.nextOption(Map(),arglist2)
  }

  /*test("main"){
    val args: Array[String] = Array("--expansionRate", "2", "--inflationRate", "2.0", "--epsilon", "0.01", "--maxIterations", "10", "--selfLoopWeight", "1", "--graphOrientationStrategy", "undirected")

    val streamIM = new java.io.ByteArrayOutputStream()
    Console.withOut(streamIM) {
      Main.main(args)
    }

    streamIM.toString.split("\n") should contain theSameElementsAs Array("0 => List(0, 1, 2, 3)", "4 => List(4, 5, 6, 7)", "9 => List(8, 9, 10)").toSeq
  }*/
}
