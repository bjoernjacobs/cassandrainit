// Basic project configuration
lazy val root = (project in file(".")).
  settings(
    name := "csup",
    organization := "com.github.bjoernjacobs",
    version := "0.0.1",
    scalaVersion := "2.11.8"
  )

// Project dependencies
libraryDependencies ++= {
  Seq(
    "com.typesafe" % "config" % "1.3.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
    "com.typesafe.akka" %% "akka-actor" % "2.4.10",
    "ch.qos.logback" %  "logback-classic" % "1.1.7",
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
  )
}