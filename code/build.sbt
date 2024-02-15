name := "formulaone"

version := "0.1"

scalaVersion := "2.12.0"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.17.3",
  "com.typesafe.akka" %% "akka-http" % "10.2.7",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.7",
  "com.typesafe.akka" %% "akka-stream" % "2.6.17"
)

libraryDependencies += "commons-net" % "commons-net" % "3.9.0"
