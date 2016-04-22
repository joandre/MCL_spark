name := "MCL_spark"

version := "1.0.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "org.apache.spark" %% "spark-graphx" % "1.6.1",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3"
)

parallelExecution in Test := false

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")