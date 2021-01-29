name := "StructuredNetworkWordCountWindowed"

version := "1.0"

scalaVersion := "2.12.12"

lazy val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming_2.12" % sparkVersion % "provided"
)
