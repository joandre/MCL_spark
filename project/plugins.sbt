addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.4.0")

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.4")