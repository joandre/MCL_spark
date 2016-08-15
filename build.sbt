// Module name
name := "MCL_spark"

// Package name
spName := "joandre/MCL_spark"

// Version
version := "1.0.0"

// License
licenses += "MIT" -> url("http://opensource.org/licenses/MIT")

// Scala version
scalaVersion := "2.11.7"

// Specify which versions of scala are allowed
crossScalaVersions := Seq("2.10.5", "2.11.7")

//Spark version
sparkVersion := "1.6.1"

// Spark dependencies
sparkComponents ++= Seq(
  "core", "sql", "mllib", "graphx"
)

// External libraries dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3"
)

// Credentials for spark package
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

// Specify multiple scala versions are published in package release
spAppendScalaVersion := true

// Disable UnitTest parallel executions for spark testing package
parallelExecution in Test := false

