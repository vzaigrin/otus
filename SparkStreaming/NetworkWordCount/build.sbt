name := "NetworkWordCount"

version := "1.0"

scalaVersion := "2.12.12"

lazy val sparkVersion = "3.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % sparkVersion % "provided"