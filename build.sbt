libraryDependencies ++= List(
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.11" % Test,
  "org.postgresql" % "postgresql" % "42.4.2" % Test,
)

scalacOptions += "-deprecation"

Test / parallelExecution := false

logBuffered := false

run / fork := true

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s", "-a")
libraryDependencies += "com.typesafe.slick" %% "slick-testkit" % "3.4.0-RC3"

dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.36"

ThisBuild / githubWorkflowBuildPreamble +=
  WorkflowStep.Run(List("docker-compose up -d"), name = Some("Start database"))
