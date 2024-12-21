import _root_.io.github.nafg.mergify.dsl.*

ThisBuild / scalaVersion := "2.13.15"
ThisBuild / scalacOptions += "-Xsource:3"

mergifyExtraConditions := Seq(
  (Attr.Author :== "scala-steward") ||
    (Attr.Author :== "slick-scala-steward[bot]") ||
    (Attr.Author :== "renovate[bot]")
)

libraryDependencies ++= List(
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "ch.qos.logback" % "logback-classic" % "1.5.15" % Test,
  "org.postgresql" % "postgresql"      % "42.7.4" % Test
)

scalacOptions += "-deprecation"

Test / parallelExecution := false

logBuffered := false

run / fork := true

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s", "-a")
libraryDependencies += "com.typesafe.slick" %% "slick-testkit" % "3.5.2"
libraryDependencies += "org.scala-lang"      % "scala-reflect" % scalaVersion.value

ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("11"))
ThisBuild / githubWorkflowBuildPreamble +=
  WorkflowStep.Run(List("docker compose up -d"), name = Some("Start database"))
