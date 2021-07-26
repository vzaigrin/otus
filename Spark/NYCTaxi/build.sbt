name := "NYCTaxi"

version := "1.0"

scalaVersion := "2.12.10"

lazy val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "provided"
)
