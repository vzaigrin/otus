name := "NYCTaxi"

version := "3.4.0"

scalaVersion := "2.12.17"

lazy val sparkVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
