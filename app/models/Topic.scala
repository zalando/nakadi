package models

import play.api.libs.json._

case class Topic(name: String)

object Topic {
  implicit val topicFormat = Json.format[Topic]
}
