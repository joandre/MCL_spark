name := "MCL_spark"

version := "0.1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.1",
  "org.apache.spark" %% "spark-sql" % "1.4.1",
  "org.apache.spark" %% "spark-mllib" % "1.4.1",
  "org.apache.spark" %% "spark-graphx" % "1.4.1"
)

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"
//libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.0"


