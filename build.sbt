import _root_.io.github.nafg.mergify.dsl._


mergifyExtraConditions := Seq(
  (Attr.Author :== "scala-steward") ||
    (Attr.Author :== "slick-scala-steward[bot]") ||
    (Attr.Author :== "renovate[bot]")
)

libraryDependencies ++= List(
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "ch.qos.logback" % "logback-classic" % "1.4.11" % Test,
  "org.postgresql" % "postgresql" % "42.6.0" % Test,
)

scalacOptions += "-deprecation"

Test / parallelExecution := false

logBuffered := false

run / fork := true

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s", "-a")
libraryDependencies += "com.typesafe.slick" %% "slick-testkit" % "3.4.1"

ThisBuild / githubWorkflowBuildPreamble +=
  WorkflowStep.Run(List("docker-compose up -d"), name = Some("Start database"))
