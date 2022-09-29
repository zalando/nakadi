package com.wiley.epdcs.eventhub.script

import com.wiley.epdcs.eventhub.data.AssignmentEventDataFeeder
import com.wiley.epdcs.eventhub.endpoint.AssignmentEvent
import io.gatling.core.Predef._

object script extends AssignmentEventDataFeeder {

  val create_assignment_event = scenario("Create_Assignment_Event")
    .feed(assignmentForDataCreation.queue)
    .exec(v1Flows.CREATEASSIGNMENT)

  object v1Flows {
    def CREATEASSIGNMENT = {
      AssignmentEvent.createAssignment
    }

  }
}
