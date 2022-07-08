name := "SparkML"

version := "1.0"

scalaVersion := "2.12.15"
lazy val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"   % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)
