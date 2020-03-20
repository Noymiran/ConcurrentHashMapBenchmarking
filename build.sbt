import pl.project13.scala.sbt.JmhPlugin

name := "PerformanceProject"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.22"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % Test

enablePlugins(JmhPlugin)
