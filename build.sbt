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
  val cassandraDriverV = "3.1.0"

  Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverV
  )
}
