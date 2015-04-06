name := "MCL_spark"

version := "0.1.0"

scalaVersion := "2.11.6"

/*libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0",
  "org.apache.spark" %% "spark-sql" % "1.3.0",
  "org.apache.spark" %% "spark-hive" % "1.3.0",
  "org.apache.spark" %% "spark-streaming" % "1.3.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0",
  "org.apache.spark" %% "spark-streaming-flume" % "1.3.0",
  "org.apache.spark" %% "spark-mllib" % "1.3.0",
  "org.apache.spark" %% "spark-graphx" % "1.3.0"
)*/

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.0"


