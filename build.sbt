name := "MCL_spark"

version := "0.1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-sql" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
  "org.apache.spark" %% "spark-graphx" % "1.5.2"
)