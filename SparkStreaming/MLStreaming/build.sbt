name := "MLStreaming"

version := "1.0"

scalaVersion := "2.12.12"

lazy val sparkVersion = "3.1.2"
lazy val kafkaVersion = "2.8.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-mllib_2.12"                % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming_2.12"            % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion,
  "org.apache.kafka" % "kafka-clients"                   % kafkaVersion
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")       => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")   => MergeStrategy.discard
  case "reference.conf"                                 => MergeStrategy.concat
  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
  case _                                                => MergeStrategy.first
}
