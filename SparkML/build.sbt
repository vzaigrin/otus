name := "SparkML"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"        % "3.0.1" % "provided",
  "org.apache.spark"  % "spark-mllib_2.12" % "3.0.1" % "provided"
)
