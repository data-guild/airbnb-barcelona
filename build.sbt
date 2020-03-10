name := "airbnb-barcelona"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.hadoop" % "hadoop-aws" % "2.8.5" % Provided,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test

)
