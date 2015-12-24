package de.zalando.nakadi.controllers

import java.util.concurrent.TimeUnit
import javax.inject.Inject

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import de.zalando.nakadi.models.CommonTypes._
import de.zalando.nakadi.models.{Metrics, Partition, Topic}
import de.zalando.nakadi.utils.ActorCommands._
import de.zalando.nakadi.utils.{KafkaClient, KafkaConsumerActor}
import org.apache.kafka.clients.consumer.KafkaConsumer
import play.api.Logger
import play.api.libs.iteratee.{Enumeratee, Enumerator}
import play.api.libs.json.{Json, Writes}
import play.api.mvc._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try

class EventBrokerController @Inject()()
                                     (implicit ec: ExecutionContext) extends Controller {

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  implicit val defaultCodec = Codec.utf_8

  val getMetrics = Action { result =>
    Ok(
      Json.toJson(
        Metrics(
          application_name = "Nakadi Event Bus",
          kafka_servers = ConfigFactory.load().getString("kafka.host").split(','), // fast and dirty
          zookeeper_servers = ConfigFactory.load().getString("zookeeper.host").split(',') // fast and dirty
        ))
    ).as(JSON)
  }

  def getTopics = withConsumer { consumer =>
    consumer.listTopics().asScala.keys.map(Topic.apply)
  }

  def getPartitionsForTopic(topic: String) = withConsumer { consumer =>
    consumer.partitionsFor(topic).asScala.map { partitionInfo =>
      Partition(
        name = partitionInfo.partition().toString,
        oldest_available_offset = "UNKNOWN",
        newest_available_offset = "UNKNOWN" )
    } sortBy(_.name)
  }

  def getEventsFromTopic(topic: String) = play.mvc.Results.TODO

  def postEventsIntoTopic(topic: String) = play.mvc.Results.TODO

  val actorSystem = ActorSystem.create("nakadi")

  def getEventsFromPartition(topic: TopicName,
                             partition: PartitionName,
                             start_from: PartitionOffset,
                             stream_limit: Option[Int] = None
                            ) = Action.async
  {
    val actor = actorSystem.actorOf(
      Props(classOf[KafkaConsumerActor], topic, partition, start_from, stream_limit.getOrElse(0)))

    val newLiner = Enumeratee.map[String] ( _ + '\n' )

    implicit val timeout = akka.util.Timeout(5, TimeUnit.SECONDS)
    akka.pattern.ask(actor, Start).mapTo[Enumerator[String]].map { events =>
      Results.Ok.chunked(events &> newLiner)
    }
  }

  private def withConsumer[A](f: KafkaConsumer[_, _] => A)(implicit w: Writes[A]): Action[AnyContent] = {
    Action {
      val consumer = KafkaClient()
      try {
        Results.Ok {
          Json.toJson {
            f(consumer)
          }
        }
      } finally {
        Try(consumer.close())
      }
    }
  }

}
