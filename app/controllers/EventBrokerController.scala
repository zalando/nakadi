package controllers

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import javax.inject.Inject

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import com.typesafe.config.ConfigFactory
import models.CommonTypes.{PartitionName, PartitionOffset, TopicName}
import models.{Metrics, _}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import play.api.Logger
import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee.Input.Empty
import play.api.libs.iteratee._
import play.api.libs.json.{Json, Writes}
import play.api.mvc.{Action, Controller, _}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, blocking}
import scala.util.Try

// define Kafka Simple Consuming Actor commands
object ActorCommands extends Enumeration {
  val Start, Next, Shutdown = Value
}
import controllers.ActorCommands._

class EventBrokerController @Inject() ()
                                   (implicit ec: ExecutionContext) extends Controller {

  val LOG = Logger("EventBrokerController")

  implicit val defaultCodec = Codec.utf_8

  val getMetrics = Action { result =>
    Ok( Json.toJson(Metrics(
      application_name = "Nakadi Event Bus",
      kafka_servers = ConfigFactory.load().getString("kafka.host").split(','), // fast and dirty
      zookeeper_servers = ConfigFactory.load().getString("zookeeper.host").split(',') // fast and dirty
      )))
      .as(JSON)
  }

  def getTopics = withConsumer { consumer =>
    consumer.listTopics().asScala.keys.map(Topic.apply)
  }

  def getPartitionsForTopic(topic: String) = withConsumer { consumer =>
    consumer.partitionsFor(topic).asScala.map { pi =>
      Partition(
        name = pi.partition().toString,
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

object KafkaClient {

  val LOG = Logger("application.KafkaClient")

  def apply() = new KafkaConsumer[String, String]({
    val set = ConfigFactory.load().getConfig("kafka.consumer").entrySet().asScala
    val props = set.foldLeft(new Properties) {
      (p, item) =>
        val key = item.getKey
        val value = item.getValue.unwrapped().toString
        require(p.getProperty(key) == null, s"key ${item.getKey} exists more than once")
        p.setProperty(key, value)
        LOG.debug(s"Read key ${key} with value ${value}")
        p
    }
    // props.setProperty("group.id", UUID.randomUUID().toString)
    props
  })
}


class KafkaConsumerActor(topic: String, partition: String, cursor: String, streamLimit: Int) extends Actor {

  val LOG = Logger("application.KafkaConsumerActor")

  lazy val consumer = KafkaClient()

  val topicPartition = new TopicPartition(topic, partition.toInt)

  @volatile var channel: Channel[String] = null
  @volatile var done: Boolean = false

  var remaining = streamLimit

  override def receive: Receive = {
    case Start =>
      LOG.debug("Got Start signal")
      blocking(consumer.assign(util.Arrays.asList(topicPartition)))
      LOG.debug(s"Assigned partition ${topicPartition}")
      blocking(consumer.seek(topicPartition, cursor.toLong))
      LOG.debug(s"Seeked to ${cursor} in partition ${topicPartition}")
      sender ! Concurrent.unicast((c: Channel[String]) => {
        this.channel = c
        self ! Next
      }).onDoneEnumerating({
        LOG.debug("Done enumerating")
        done = true
        self ! Shutdown
      })
    case Next =>
      LOG.debug("Got Next signal")

      if (done) {
        LOG.debug("Done consuming from the channel")
      } else {
        // annoyingly, poll will block more or less forever if kafka isn't running
        val records = blocking(consumer.poll(500).asScala)

        LOG.debug(s"got ${records.size} event from the queue")

        if (records.isEmpty) {
          // push empty element to be able to react on connection drop
          channel.push(Empty)
          self ! Next
        } else {
          val toStream = if (streamLimit == 0) records else records.take(math.min(remaining, records.size))
          toStream.foreach { r => channel push r.value }
          remaining -= toStream.size
          assert(remaining >= 0, remaining)
          if (streamLimit == 0 || remaining > 0) self ! Next
          else channel.eofAndEnd()
        }
      }
    case Shutdown =>
      LOG.debug("Got Shutdown signal")
      self ! PoisonPill
      Try(consumer.close())
  }
}
