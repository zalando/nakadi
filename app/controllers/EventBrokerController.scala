package controllers

import javax.inject.Inject

import play.api.mvc.Controller

import scala.concurrent.ExecutionContext

class EventBrokerController @Inject() ()
                                   (implicit ec: ExecutionContext) extends Controller {

  def getMetrics = {
    play.mvc.Results.TODO
  }

  def getTopics = play.mvc.Results.TODO
  def getPartitionsForTopic(topic: String) = play.mvc.Results.TODO
  def getEventsFromTopic(topic: String) = play.mvc.Results.TODO
  def getEventsFromPartition(topic: String, partition: String) = play.mvc.Results.TODO

  def postEventsIntoTopic(topic: String) = play.mvc.Results.TODO
}

