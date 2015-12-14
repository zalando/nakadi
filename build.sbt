name := """nakadi"""

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

// scalacOptions ++= Seq("-feature", "-deprecation")
// scalacOptions in Test ++= Seq("-Yrangepos")

//parallelExecution in Test := true

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  specs2 % Test
)

routesGenerator := InjectedRoutesGenerator
fork in run := true

// scoverage

import scoverage.ScoverageSbtPlugin.ScoverageKeys._

coverageExcludedPackages := "<empty>;Reverse.*;views.json..*"
coverageMinimum := 75
coverageFailOnMinimum := true

