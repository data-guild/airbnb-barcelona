name := "airbnb-barcelona"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % "2.10.0",
  "org.apache.hadoop" % "hadoop-common" % "2.10.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)
