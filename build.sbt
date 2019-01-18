name := "hbase-exporter"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

// Spark libs
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided"

// Hbase libs
libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "23.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0",
  "org.apache.hbase" % "hbase-common" % "1.3.1",
  "org.apache.hbase" % "hbase-client" % "1.3.1"
)

libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

libraryDependencies ++= {
  val circeV = "0.8.0"
  Seq(
    "io.circe" %% "circe-core" % circeV,
    "io.circe" %% "circe-generic" % circeV,
    "io.circe" %% "circe-parser" % circeV,
    "io.circe" %% "circe-java8" % circeV,
    "io.circe" %% "circe-generic-extras" % circeV,
    "io.circe" %% "circe-yaml" % circeV
  )
}

// Merging strategy
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)