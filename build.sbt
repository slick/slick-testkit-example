import _root_.io.github.nafg.mergify.dsl.*

ThisBuild / scalaVersion := "3.10.0"

mergifyExtraConditions := Seq(
  (Attr.Author :== "scala-steward") ||
    (Attr.Author :== "slick-scala-steward[bot]") ||
    (Attr.Author :== "renovate[bot]")
)

libraryDependencies ++= List(
  "com.github.sbt" % "junit-interface" % "0.13.3"  % Test,
  "ch.qos.logback" % "logback-classic" % "1.6.6"   % Test,
  "org.postgresql" % "postgresql"      % "42.7.14" % Test
)

scalacOptions += "-deprecation"

Test / parallelExecution := false

logBuffered := false

run / fork := true

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s", "-a")
libraryDependencies += "com.typesafe.slick" %% "slick-testkit" % "3.6.1"

ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("17"))
ThisBuild / githubWorkflowBuildPreamble +=
  WorkflowStep.Run(List("docker compose up -d"), name = Some("Start database"))

// In sbt 2, `test` only reruns tests affected by changes since the cached last run,
// so on CI it can skip the whole suite.
ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Sbt(List("testFull"), name = Some("Build project"))
)

ThisBuild / githubWorkflowPublishTargetBranches := Seq()
