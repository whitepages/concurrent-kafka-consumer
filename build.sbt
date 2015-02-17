import sbt._
import Keys._

name := "concurrent-kafka-consumer"

organization := "com.whitepages"

repo := "search-dev"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.11.2")   // sbt-release bug!

wpSettings

fork in test := false

fork in run := true

resolvers += "whitepages-snapshots" at "http://jrepo0.dev.pages:8081/artifactory/whitepages-snapshots"

libraryDependencies ++= Seq(
  // this should get pulled in by wp-kafka-consumer-scala, but something screws up in the test context without the explicit inclusion
  "org.apache.kafka" %% "kafka"                 % "0.8.2.0",
  "com.whitepages" %% "wp-kafka-consumer-scala" % "0.0.7-SNAPSHOT",
  "com.whitepages" %% "scala-test"              % "9.0.2"      % "test",
  "org.apache.kafka" %% "kafka"                 % "0.8.2.0" % "test" classifier "test"
)

javaOptions ++= Seq("-Xmx2048m", "-XX:StringTableSize=1000003")




