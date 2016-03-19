name := "MCL_spark"

version := "1.0.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.apache.spark" %% "spark-graphx" % "1.6.0",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.1"
)

parallelExecution in Test := false