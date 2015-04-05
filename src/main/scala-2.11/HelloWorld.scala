// import required spark classes
import org.apache.spark.{SparkConf, SparkContext}

// define main method (scala entry point)
object HelloWorld {
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HelloWorld")
      .set("spark.executor.memory","1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction","1")

    val sc = new SparkContext(conf)
    // do stuff
    println("Hello, world!")
    // terminate spark context
    sc.stop()
  }
}