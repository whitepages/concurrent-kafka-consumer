import sbt._
import Keys._

name := "concurrent-kafka-consumer"

organization := "com.whitepages"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.11.2")   // sbt-release bug!

fork in test := false

fork in run := true

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka"                 % "0.8.2.0",
  "org.scalatest" %% "scalatest" % "2.1.7",
  "org.apache.kafka" %% "kafka"                 % "0.8.2.0" % "test" classifier "test"
)

javaOptions ++= Seq("-Xmx2048m", "-XX:StringTableSize=1000003")




