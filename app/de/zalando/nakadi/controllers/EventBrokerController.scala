package de.zalando.nakadi.controllers

import javax.inject.Inject

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import de.zalando.nakadi.models.CommonTypes._
import de.zalando.nakadi.models._
import de.zalando.nakadi.utils.{RawKafkaConsumer, RawKafkaProducer, StreamingKafkaConsumer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import play.api.Logger
import play.api.libs.EventSource
import play.api.libs.iteratee._
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


//  val concatLine: Iteratee[Parsing.MatchInfo[Array[Byte]],String] =
//    ( // stop on the match
//      Enumeratee.breakE[Parsing.MatchInfo[Array[Byte]]](_.isMatch) ><>
//        // then pick the unmatched bytes and convert them to string
//        Enumeratee.collect{
//          case Parsing.Unmatched(bytes) =>
//            val s = new String(bytes, StandardCharsets.UTF_8)
//            // LOG.debug(s"collected string '${s}'")
//            s
//        } &>> Iteratee.consume()
//    ).flatMap { r =>
//      // LOG.debug(s"flatmap after consume: ${r}")
//      Iteratee.head.map(_ => r)
//    }
//
//  val newLineParser: Iteratee[Array[Byte], List[String]] =
//    Parsing.search("\n".getBytes) ><>
//      Enumeratee.grouped( concatLine ) &>>
//      Iteratee.head.flatMap { header =>
//        LOG.debug(s"before chunking the header is: ${header}")
//        Iteratee.getChunks
//      }
//
//
//  val newLineRegrouper: Enumeratee[Array[Byte], String] = Enumeratee.grouped {
//    play.api.libs.iteratee.Traversable.splitOnceAt[Array[Byte], Byte]( _ == '\n') &>> Iteratee.consume()
//  } ><> Enumeratee.map { bytes => new String(bytes, StandardCharsets.UTF_8) }


  val newLineProcessor: Iteratee[Array[Byte], Enumerator[JsValue]] = {
    Enumeratee.grouped {
      // re-group byte arrays, splitting them on new lines
      play.api.libs.iteratee.Traversable.splitOnceAt[Array[Byte], Byte]( _ != '\n'.toByte ) &>> Iteratee.consume()
    } ><> Enumeratee.map { bytes => Try(Json.parse(bytes)).getOrElse(Json.obj( "error" -> "cannot parse")) } &>>
      Iteratee.fold(Enumerator.empty[JsValue]) // this interatee consumes everything from the HTTP stream and this is wrong :(
      { (e, s) =>
        LOG.debug(s"newLineProcessor got new string '${s}'")
        // if ( s. ) e
        e andThen Enumerator(s)
      }
  }


  val streamingJSONStreamBodyParser = BodyParser("Streaming JSON body parser") { request =>
    newLineProcessor.map(Right(_))
  }

  def getEventsFromTopic(topic: String) = play.mvc.Results.TODO

  def postEventsIntoTopicAsync(topic: String) = Action(streamingJSONStreamBodyParser) { request =>
    val logger = Enumeratee.map[JsValue]{ s =>
      LOG.debug(s"got line '${s}'")
      s
    }
    val loggerE = Enumeratee.map[EventSource.Event]{ e =>
      LOG.debug(s"got event '${e}'")
      e
    }
    val e = request.body
    Results.Ok.chunked {
      e through logger through (EventSource()) through loggerE
    }.as("text/event-stream")
  }

  def postEventsIntoTopic(topic: String) = withProducer { (request, producer) =>
    request.body.asJson.map { (body: JsValue) =>
      body.validate[Seq[Event]].asOpt.map { s =>
        import java.util.concurrent.{Future => JFuture}

        Future.sequence( s.map { event =>
            LOG.debug(s"sending event ${event}")
            val r: JFuture[RecordMetadata] = producer.send(new ProducerRecord[String, String](topic,null, Json.toJson(event).toString))
            Future{
              val result = blocking(r.get())
              LOG.debug(s"published event ${event} into partion ${result.partition()} at offset ${result.offset()}")
              result
            }
          }
        ).map{ r =>  // future
          LOG.debug(s"got result from all the futures as ${r}")
          Json.toJson( r.groupBy( rm => String.valueOf( rm.partition() ) ).map { case (partition, s) => partition -> s.size } )
        }
      }.getOrElse {
        LOG.error("Could not parse body as exptected structure")
        Future.failed(new Exception("Could not parse body as expected structure"))
      }
    }.getOrElse {
      LOG.error("Could not parse the body as JSON")
      Future.failed(new Exception("Could not parse the body as JSON"))
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
      lazy val producer = RawKafkaProducer[String, String]
      try {
        f(result, producer).map { r => Results.Ok(Json.toJson(r)) } recover { case t: Throwable => Results.BadRequest(t.getMessage) }
      } finally {
        Try(producer.close())
      }
    }
  }
}
