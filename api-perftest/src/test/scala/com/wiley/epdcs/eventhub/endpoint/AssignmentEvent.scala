package com.wiley.epdcs.eventhub.endpoint

import com.typesafe.config.Config
import com.wiley.epdcs.eventhub.test.AssignmentProgressConfig
import io.gatling.core.Predef.{ElFileBody, jsonPath, _}
import io.gatling.http.Predef._

object AssignmentEvent {

  def config(c: Config) =
    AssignmentProgressConfig(
      c.getString("assignment.baseUrl")
    )

  def createAssignment = {
    http("01 Create assignment event")
      .post("/event-types/assignment.created/events")
      .body(ElFileBody("createAssignment.json"))
      .check(status.is(200))
  }
}
