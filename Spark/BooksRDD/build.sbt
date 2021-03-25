name := "BooksRDD"

version := "1.0"

scalaVersion := "2.12.12"

lazy val sparkVersion  = "3.0.1"
lazy val hadoopVersion = "2.7.4"
lazy val circeVersion  = "0.13.0"
lazy val csvVersion    = "1.8"

libraryDependencies ++= Seq(
  "org.apache.spark"   % "spark-core_2.12" % sparkVersion  % "provided",
  "org.apache.commons" % "commons-csv"     % csvVersion,
  "org.apache.hadoop"  % "hadoop-hdfs"     % hadoopVersion % "provided",
  "io.circe"          %% "circe-core"      % circeVersion,
  "io.circe"          %% "circe-generic"   % circeVersion,
  "io.circe"          %% "circe-parser"    % circeVersion
)
