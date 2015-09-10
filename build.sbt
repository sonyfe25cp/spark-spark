name := "sparkspark"

version := "1.0"

scalaVersion := "2.10.5"


libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10"   % "1.4.0",
  "javax.servlet"           % "javax.servlet-api"       %        "3.1.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.4.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.4.0"
)