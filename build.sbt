organization := "org.zalando"

organizationHomepage := Some(url("https://tech.zalando.org"))

name := """nakadi"""

homepage := Some(url("http://www.github.com/zalando/nakadi"))

startYear := Some(2015)

version := "0.1-SNAPSHOT"


scalaVersion := "2.11.7"

// scalacOptions ++= Seq("-feature", "-deprecation")
// scalacOptions in Test ++= Seq("-Yrangepos")

//parallelExecution in Test := true

testOptions in Test += Tests.Setup { () =>
  val log = streams.value.log
  log.info("Starting local Kafka")
  """make -C local-test run""" !!
}

testOptions in Test += Tests.Cleanup { () =>
  val log = streams.value.log
  log.info("Stopping local Kafka")
  """make -C local-test kill""" !!
}

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.9.+", // excludeAll(ExclusionRule(organization = "org.slf4j")),
  specs2 % Test
)

routesGenerator := InjectedRoutesGenerator
fork in run := true

// scoverage
import scoverage.ScoverageSbtPlugin.ScoverageKeys._

coverageExcludedPackages := "<empty>;Reverse.*;views.json..*"
coverageMinimum := 75
coverageFailOnMinimum := true

