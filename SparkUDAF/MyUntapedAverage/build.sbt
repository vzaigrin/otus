ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "MyUntypedAverage"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
)
