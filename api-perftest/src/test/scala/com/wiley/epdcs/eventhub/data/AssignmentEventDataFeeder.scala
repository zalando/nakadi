package com.wiley.epdcs.eventhub.data

trait AssignmentEventDataFeeder {

  lazy val assignmentForDataCreation = for {
    abc <- 1 to 10000
  } yield {
    def uuid = java.util.UUID.randomUUID.toString
    Map(
      "assignmentId" -> s"urn:wiley:edpub:learninginstance:$uuid",
      "eid" -> s"$uuid"
    )
  }

}
