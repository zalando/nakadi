package models

import java.util.{Date, UUID}

import models.CommonTypes._
import play.api.libs.json._

object CommonTypes {
  type TopicName = String
  type PartitionName = String
  type PartitionOffset = String
  type EventId = UUID
  type Scope = String
}

case class Metrics(application_name: String)

object Metrics {
  implicit val metricsFormat = Json.format[Metrics]
}


case class Topic(name: TopicName)

object Topic {
  implicit val topicFormat = Json.format[Topic]
}

case class Partition(name: PartitionName,
                     oldestAvailableOffset: PartitionOffset,
                     newestAvailableOffset: PartitionOffset
                    )

object Partition {
  implicit val partitionFormat = Json.format[Partition]
}

case class EventMetaData(
                         id: EventId,
                         created: Date,
                         root_id: Option[EventId],
                         scopes: Option[Seq[Scope]]
                        )

object EventMetaData {
  implicit val eventMetaData = Json.format[EventMetaData]
}

case class Event(event_type: String,
                 partitioning_key: String,
                 metadata: EventMetaData
                )

object Event {
  implicit val eventFormat = Json.format[Event]
}

case class PartitionCursor(partition: PartitionName, offset: PartitionOffset)

object PartitionCursor {
  implicit val partitionCursorFormat = Json.format[PartitionCursor]
}

case class SimpleStreamEvent(cursor: PartitionCursor, events: Seq[Event])

object SimpleStreamEvent {
  implicit val simpleStreamEvent = Json.format[SimpleStreamEvent]
}