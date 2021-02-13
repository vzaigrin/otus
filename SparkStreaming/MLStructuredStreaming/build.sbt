name := "MLStructuredStreaming"

version := "1.0"

scalaVersion := "2.12.12"

lazy val sparkVersion = "3.0.1"
lazy val kafkaVersion = "2.7.0"

libraryDependencies ++= Seq(
  "com.typesafe"      % "config"                    % "1.4.0",
  "org.apache.spark" %% "spark-sql"                 % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib"               % sparkVersion % "provided",
  "org.apache.spark"  % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.kafka"  % "kafka-clients"             % kafkaVersion
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")       => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")   => MergeStrategy.discard
  case "reference.conf"                                 => MergeStrategy.concat
  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
  case _                                                => MergeStrategy.first
}
