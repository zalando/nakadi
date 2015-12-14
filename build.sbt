name := "nakadi"

version := "0.1"

scalaVersion := "2.11.7"
scalacOptions ++= Seq("-feature", "-deprecation")
scalacOptions in Test ++= Seq("-Yrangepos")

parallelExecution in Test := true

resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  // "com.typesafe.akka" %% "akka-actor" % "2.4.1",
  // "com.jsuereth" % "scala-arm" % "1.4",
  "com.gilt" %% "play-json-service-lib-2-3" % "1.1.0",
  specs2 % "test"
)

lazy val root = (project in file(".")).enablePlugins(PlayScala, SbtTwirl)

TwirlKeys.templateFormats += ("json" -> "com.gilt.play.json.templates.JsonFormat")

// scoverage

import scoverage.ScoverageSbtPlugin.ScoverageKeys._

coverageExcludedPackages := "<empty>;Reverse.*;views.json..*"
coverageMinimum := 75
coverageFailOnMinimum := true

