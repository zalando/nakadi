package de.zalando.nakadi.controllers

import javax.inject.Inject

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import de.zalando.nakadi.models.CommonTypes._
import de.zalando.nakadi.models.{Metrics, Partition, PartitionCursor, Topic}
import de.zalando.nakadi.utils.{RawKafkaConsumer, RawKafkaProducer, StreamingKafkaConsumer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import play.api.Logger
import play.api.libs.iteratee.Enumeratee
import play.api.libs.json.{JsValue, Json, Writes}
import play.api.mvc._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
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
        //TODO: implemnet fetching olders and newest offsets
        oldest_available_offset = "UNKNOWN",
        newest_available_offset = "UNKNOWN")
    } sortBy (_.name)
  }

  def getEventsFromTopic(topic: String) = play.mvc.Results.TODO

  def postEventsIntoTopic(topic: String) = withProducer { (request, producer) =>
    request.body.asJson.map { (body: JsValue) =>

      import java.util.concurrent.{Future => JFuture}

      val r: JFuture[RecordMetadata] = producer.send(new ProducerRecord[String, String](topic, "dummy"))
      val sr: Future[RecordMetadata] = Future( blocking(r.get()) )
      Future.successful("OK")
    }.getOrElse {
      Future.failed(new Exception("Could not parse the body"))
    }
  }

  implicit val actorSystem = ActorSystem.create("nakadi")

  import RawKafkaConsumer.{kafkaKeyDeserializer, kafkaValueDeserializer}

  def getEventsFromPartition(topic: TopicName,
                             partition: PartitionName,
                             start_from: PartitionOffset,
                             stream_limit: Option[Int] = None
                            ) = Action.async {
    implicit val consumer = RawKafkaConsumer[String, String]

    val rawStream = StreamingKafkaConsumer.rawStream[String, String](
      topic,
      Seq(PartitionCursor(partition, start_from)),
      stream_limit
    )

    val newLiner = Enumeratee.map[ConsumerRecord[String, String]] { r =>
      r.value() + '\n'
    }

    rawStream.map { events =>
      Results.Ok.chunked(events &> newLiner)
    }
  }

  private def withConsumer[A](f: KafkaConsumer[String, String] => A)(implicit w: Writes[A]): Action[AnyContent] = {
    Action {
      import RawKafkaConsumer._
      val consumer = RawKafkaConsumer[String, String]
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

  private def withProducer[A](f: (Request[AnyContent], KafkaProducer[String, String]) => Future[A])(implicit w: Writes[A]): Action[AnyContent] = {
    Action.async { result =>
      import RawKafkaProducer._
      val producer = RawKafkaProducer[String, String]
      try {
        f(result, producer).map { r => Results.Ok(Json.toJson(r)) } recover { case t: Throwable => Results.BadRequest(t.getMessage) }
      } finally {
        Try(producer.close())
      }
    }
  }
}
