// Basic project configuration
lazy val root = (project in file(".")).
  settings(
    name := "csup",
    organization := "com.github.bjoernjacobs",
    version := "0.0.6-SNAPSHOT",
    scalaVersion := "2.12.1"
  )

// Project dependencies
libraryDependencies ++= {
  Seq(
    "com.typesafe" % "config" % "1.3.1",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "io.monix" %% "monix" % "2.2.2",
    //"ch.qos.logback" %  "logback-classic" % "1.1.7",
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.4"
  )
}

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }

pomExtra := <url>https://github.com/bjoernjacobs/csup</url>
  <licenses>
    <license>
      <name>Apache 2.0</name>
      <url>https://opensource.org/licenses/Apache-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:bjoernjacobs/csup.git</url>
    <connection>scm:git:git@github.com:bjoernjacobs/csup.git</connection>
  </scm>
  <developers>
    <developer>
      <id>bjacobs</id>
      <name>Bjoern Jacobs</name>
      <url>https://www.codecentric.de/team/profil/?profil=bjoern-jacobs</url>
    </developer>
  </developers>