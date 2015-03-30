name := "slick-testkit-example"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= List(
  "com.typesafe.slick" %% "slick" % "3.0.0-RC2",
  "com.typesafe.slick" %% "slick-testkit" % "3.0.0-RC2" % "test",
  "com.novocode" % "junit-interface" % "0.10" % "test",
  "ch.qos.logback" % "logback-classic" % "0.9.28" % "test",
  "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test"
)

// Disable all other test frameworks to silence the warnings in Activator
testFrameworks := Seq(new TestFramework("com.novocode.junit.JUnitFramework"))

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s", "-a")

parallelExecution in Test := false

logBuffered := false


fork in run := true