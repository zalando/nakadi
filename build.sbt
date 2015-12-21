import scala.util.{Try,Success,Failure}

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

val dockerMachineIp = taskKey[String]("An IP address of the docker-machine or localhost if docker-machine is not there")

dockerMachineIp in Test := {
  val log = streams.value.log
  log.info("Fetching docker machine IP address")
  Try("docker-machine ip default" !!) match {
    case Success(ip) => ip.trim
    case Failure(e) =>
      log.info("could not get docker-machine IP address, falling back to localhost")
      "localhost"
  }
}

fork in (Test, run) := true
envVars in Test += ( "ZOOKEEPER_HOST" -> (dockerMachineIp in Test).value )
envVars in Test += ( "KAFKA_HOST" -> (dockerMachineIp in Test).value )

testOptions in Test += Tests.Setup { () =>
  val log = streams.value.log
  log.info("Starting local Kafka")
  """make -C local-test run""" !
}

testOptions in Test += Tests.Cleanup { () =>
  val log = streams.value.log
  log.info("Stopping local Kafka")
  """make -C local-test kill""" !
}

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.9.+", // excludeAll(ExclusionRule(organization = "org.slf4j")),
  specs2 % Test
)

routesGenerator := InjectedRoutesGenerator

// scoverage
import scoverage.ScoverageSbtPlugin.ScoverageKeys._

coverageExcludedPackages := "<empty>;Reverse.*;views.json..*"
coverageMinimum := 75
coverageFailOnMinimum := false

