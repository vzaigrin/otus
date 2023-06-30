name := "CreditCardCustomers"

version := "1.0"

scalaVersion := "2.12.17"
lazy val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"   % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)
