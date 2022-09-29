package  com.wiley.epdcs.eventhub.test

import com.wiley.epdcs.eventhub.core.ConfigProvider
import com.wiley.epdcs.eventhub.data.AssignmentEventDataFeeder
import com.wiley.epdcs.eventhub.endpoint.AssignmentEvent
import com.wiley.epdcs.eventhub.script.script
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef.http

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class EventHubLoadTest extends Simulation with ConfigProvider with AssignmentEventDataFeeder {

  implicit val assignmentProgressConfig: AssignmentProgressConfig = AssignmentEvent.config(config)

  private final def testDataPreparationDuration: Int = getProperty("TEST_DATA_PREPARATION_DURATION", "5").toInt

  val httpConf = http
    .baseUrl("https://eventhub.dev.tc.private.wiley.host")
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")
    .disableCaching
    .headers(Map(
      "Transaction-Id" -> "2eecea7d-0309-46ca-81e6-c2dd287ac54f"
    ))
    .warmUp("https://eventhub.dev.tc.private.wiley.host")
    .shareConnections

  private final def getProperty(propertyName: String, defaultValue: String): String = {
    Option(System.getenv(propertyName)).orElse(Option(System.getProperty(propertyName))).getOrElse(defaultValue)
  }

  setUp(
    script.create_assignment_event.inject(
      rampUsersPerSec(1).to(5).during(2 minutes),
      constantUsersPerSec(5).during(1 minutes),
      rampUsersPerSec(5).to(1).during(2 minutes)
    )
  ).protocols(httpConf)
}

case class AssignmentProgressConfig(baseUrl: String)
