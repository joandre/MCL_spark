name := "MCL_spark"

version := "0.1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.apache.spark" %% "spark-graphx" % "1.6.0",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
)

