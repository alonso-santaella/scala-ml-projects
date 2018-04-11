name := "scalamlprojects"

version := "0.1"

organization := "com.alonsosantaella"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
)