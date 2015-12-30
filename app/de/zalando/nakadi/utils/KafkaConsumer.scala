package de.zalando.nakadi.utils

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import de.zalando.nakadi.models.CommonTypes.TopicName
import de.zalando.nakadi.models.PartitionCursor
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee.Input.Empty
import play.api.libs.iteratee.{Concurrent, Enumerator}

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util.Try


trait PropertyExtractor {

  def LOG: Logger

  protected def propertyRoot: String

  protected def extractProperties(): Properties = {
    val config = ConfigFactory.load().getConfig(propertyRoot).entrySet().asScala
    val props = config.foldLeft(new Properties) {
      (p, item) =>
        val key = item.getKey
        val value = item.getValue.unwrapped().toString
        require(p.getProperty(key) == null, s"key ${item.getKey} exists more than once")
        p.setProperty(key, value)
        LOG.debug(s"Read key ${key} with value ${value}")
        p
    }
    props
  }
}

object RawKafkaConsumer extends PropertyExtractor {

  implicit val kafkaKeyDeserializer = new UTF8StringKeyDeserializer
  implicit val kafkaValueDeserializer = new UTF8StringValueDeserializer

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  val propertyRoot = "kafka.consumer"

  def apply[K, V](implicit keyDeserializer: KeyDeserializer[K], valueDeserializer: ValueDeserializer[V]) = {
    new KafkaConsumer[K, V](extractProperties, keyDeserializer, valueDeserializer)
  }

}

object RawKafkaProducer extends PropertyExtractor {

  implicit val kafkaKeySerializer = new UTF8StringKeySerializer
  implicit val kafkaValueSerializer = new UTF8StringValueSerializer

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  val propertyRoot = "kafka.producer"

  def apply[K, V](implicit keySerializer: KeySerializer[K], valueSerializer: ValueSerializer[V]) = {
    new KafkaProducer[K, V](extractProperties, keySerializer, valueSerializer)
  }

}


class StreamingKafkaConsumer[K, V](private val rawConsumer: KafkaConsumer[K, V]) {

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  def topicNames(): Iterable[String] = rawConsumer.listTopics().asScala.keys

  def partitionNamesFor(topicName: String): Iterable[String] = rawConsumer.partitionsFor(topicName).asScala.map(_.partition.toString)

}


object StreamingKafkaConsumer {

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  def rawStream[K, V](topic: TopicName,
                      partitionCursors: Seq[PartitionCursor],
                      streamLimit: Option[Int],
                      defaultTimeout: Timeout = akka.util.Timeout(5, TimeUnit.SECONDS)
                     )
                     (implicit actorSystem: ActorSystem, consumer: KafkaConsumer[K, V]): Future[Enumerator[ConsumerRecord[K, V]]] = {
    val actor = actorSystem.actorOf(
      // doing it using direct constructor invocation to be able to pass an implicit consumer
      Props(new KafkaConsumerActor[K, V](topic, partitionCursors, streamLimit))
    )
    implicit val timeout = defaultTimeout
    akka.pattern.ask(actor, ActorCommands.Start).mapTo[Enumerator[ConsumerRecord[K, V]]]
  }
}

private[nakadi] object ActorCommands extends Enumeration {
  val Start, Next, Shutdown = Value
}

import de.zalando.nakadi.utils.ActorCommands._

private[nakadi] class KafkaConsumerActor[K, V](topic: TopicName,
                                               partitionCursors: Seq[PartitionCursor],
                                               streamLimit: Option[Int])
                                              (implicit consumer: KafkaConsumer[K, V]) extends Actor {

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  val rawPartitions = partitionCursors.map { pc => new TopicPartition(topic, pc.partition.toInt) }.asJava

  @volatile var channel: Channel[ConsumerRecord[K, V]] = null
  @volatile var done: Boolean = false

  var remaining = streamLimit.getOrElse(0)

  override def receive: Receive = {
    case Start =>
      LOG.debug("Got Start signal")

      blocking(consumer.assign(rawPartitions))
      LOG.debug(s"Assigned partition ${rawPartitions}")

      partitionCursors.map { pc =>
        blocking(consumer.seek(new TopicPartition(topic, pc.partition.toInt), pc.offset.toLong))
        LOG.debug(s"Seeked to ${pc.offset} in partition ${pc.partition}")
      }
      sender ! Concurrent.unicast[ConsumerRecord[K, V]](
        onStart = { c =>
          this.channel = c
          self ! Next
        },
        onError = { (e, i) =>
          LOG.error(s"Got an error in channel: ${e}")
        }
      ).onDoneEnumerating({
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
          val eventsToDeliver = if (remaining == 0) records else records.take(math.min(remaining, records.size))
          eventsToDeliver.foreach { r => channel push r }
          remaining = remaining - eventsToDeliver.size
          LOG.debug(s"pushed ${eventsToDeliver.size} events to the channel, remaining ${remaining} events until end of stream")
          if ( streamLimit.exists(_ > 0) && remaining <= 0) {
            channel.eofAndEnd()
          } else {
            self ! Next
          }
        }
      }
    case Shutdown =>
      LOG.debug("Got Shutdown signal")
      self ! PoisonPill
      Try(consumer.close())
  }
}
