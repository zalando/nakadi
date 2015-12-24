package de.zalando.nakadi.utils

import java.util.Properties

import akka.actor.{Actor, PoisonPill}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import play.api.Logger
import play.api.libs.iteratee.Concurrent
import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee.Input.Empty

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util.Try
import play.api.libs.concurrent.Execution.Implicits._


object KafkaClient {

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  def apply() = {
    val consumerProps = {
      val config = ConfigFactory.load().getConfig("kafka.consumer").entrySet().asScala
      val props = config.foldLeft(new Properties) {
        (p, item) =>
          val key = item.getKey
          val value = item.getValue.unwrapped().toString
          require(p.getProperty(key) == null, s"key ${item.getKey} exists more than once")
          p.setProperty(key, value)
          LOG.debug(s"Read key ${key} with value ${value}")
          p
      }
      // props.setProperty("group.id", UUID.randomUUID().toString)
      props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props
    }
    new KafkaConsumer[String, String](consumerProps)
  }

}

class StreamingKafkaConsumer[K, V](private val rawConsumer: KafkaConsumer[K, V]) {

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  def topicNames(): Iterable[String] = rawConsumer.listTopics().asScala.keys

  def partitionNamesFor(topicName: String): Iterable[String] = rawConsumer.partitionsFor(topicName).asScala.map(_.partition.toString)

}

private[nakadi] object ActorCommands extends Enumeration {
  val Start, Next, Shutdown = Value
}
import de.zalando.nakadi.utils.ActorCommands._

private[nakadi] class KafkaConsumerActor(topic: String, partition: String, cursor: String, streamLimit: Int) extends Actor {

  val LOG = Logger(this.getClass)
  LOG.debug("logger initialized")

  lazy val consumer = KafkaClient()

  val topicPartition = new TopicPartition(topic, partition.toInt)

  @volatile var channel: Channel[String] = null
  @volatile var done: Boolean = false

  var remaining = streamLimit

  override def receive: Receive = {
    case Start =>
      LOG.debug("Got Start signal")
      blocking(consumer.assign(java.util.Arrays.asList(topicPartition)))
      LOG.debug(s"Assigned partition ${topicPartition}")
      blocking(consumer.seek(topicPartition, cursor.toLong))
      LOG.debug(s"Seeked to ${cursor} in partition ${topicPartition}")
      sender ! Concurrent.unicast[String](
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
