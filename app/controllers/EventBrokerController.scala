package controllers

import javax.inject.Inject

import models.Metrics
import play.api.libs.json.Json
import play.api.mvc.{Codec, Action, Controller}

import scala.concurrent.ExecutionContext

class EventBrokerController @Inject() ()
                                   (implicit ec: ExecutionContext) extends Controller {

  implicit val defaultCodec = Codec.utf_8

  val getMetrics = Action { result =>
    Ok( Json.toJson(Metrics(applicationName = "Nakadi Event Bus")) )
      .as(JSON)
  }

  def getTopics = play.mvc.Results.TODO
  def getPartitionsForTopic(topic: String) = play.mvc.Results.TODO
  def getEventsFromTopic(topic: String) = play.mvc.Results.TODO
  def getEventsFromPartition(topic: String, partition: String) = play.mvc.Results.TODO

  def postEventsIntoTopic(topic: String) = play.mvc.Results.TODO
}

