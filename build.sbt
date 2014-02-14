name := "Slick-TestKit-Example"

organizationName := "Typesafe"

organization := "com.typesafe.slick"

version := "2.0.1-RC1"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "2.0.1-RC1",
  "com.typesafe.slick" %% "slick-testkit" % "2.0.1-RC1" % "test",
  "com.novocode" % "junit-interface" % "0.10" % "test",
  "ch.qos.logback" % "logback-classic" % "0.9.28" % "test",
  "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test"
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s", "-a")

parallelExecution in Test := false

logBuffered := false
