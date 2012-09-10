name := "Slick-TestKit-Example"

organizationName := "Typesafe"

organization := "com.typesafe"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.0-M7"

scalaBinaryVersion in Global := "2.10.0-M7"

libraryDependencies ++= Seq(
  "com.typesafe" %% "slick" % "1.0.0-SNAPSHOT",
  "com.typesafe" %% "slick-testkit" % "1.0.0-SNAPSHOT" % "test",
  "com.novocode" % "junit-interface" % "0.10-M1" % "test",
  "ch.qos.logback" % "logback-classic" % "0.9.28" % "test",
  "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test"
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s", "-a")

parallelExecution in Test := false

logBuffered := false
