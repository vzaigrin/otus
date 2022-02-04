name := "NYCTaxi"

version := "3.2.1"

scalaVersion := "2.12.15"

lazy val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
