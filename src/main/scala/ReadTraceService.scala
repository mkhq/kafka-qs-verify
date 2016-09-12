package org.mkhq.kafka

import com.twitter.finagle.{ChannelException, Http, Service}
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.server.TwitterServer
import com.twitter.util.{Await, Future}

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.util.Properties
import util.{Failure, Success, Try}

class KeyService(streams: KafkaStreams, table: String, host: String)
extends Service[Request, Response] {
  import Topology._
  import Errors._

  private def response(status: Status, data: Option[String]): Response = {
    val resp = Response(status)
    resp.host = host

    for(d <- data) resp.contentString = d
    resp
 }

  private def readKey(key: String): Response = {
    streams.read(table, key) match {
      case Failure(error: KeyNotFound) =>
        response(Status.NotFound, data = Some(error.toString))

      case Failure(error: StoreUnavailable) =>
        response(Status.ServiceUnavailable, data = Some(error.toString))

      case Failure(error: Throwable) =>
        response(Status.InternalServerError, data = Some(error.toString))

      case Success(value) =>
        response(Status.Ok, data = Some(value.toString))
    }
  }

  def apply(req: Request): Future[Response] = {
    val key = req.uri.replace("/","")

    // 1. retrieve the value from the state store
    val resp = streams.read(table, key).map { value =>
      response(Status.Ok, data = Some(value.toString))
    } recover {
    // 2. Recovery from any errors thrown by read
      case error: KeyNotFound =>
        // verify that the current instance is responsible for the key
        streams.verifyKeyAtHost(table, key, host) match {
          case Success(_) =>
            response(Status.NotFound, data = Some(error.toString))
          case Failure(error: KeyAtOtherStore) =>
            response(Status.SeeOther, data = Some(error.toString))
        }

      case error: StoreUnknownError =>
        response(Status.ServiceUnavailable, data = Some(error.toString))

      case error: StoreUnavailable =>
        response(Status.ServiceUnavailable, data = Some(error.toString))

      case error: Throwable =>
        response(Status.InternalServerError, data = Some(error.toString))

    } getOrElse {
      response(Status.InternalServerError, data = Some("Unhandled failure"))
    }

    Future.value(resp)
  }
}

object ReadTraceService
extends TwitterServer {
  import Topology._

  val instanceIdFlag = flag("instance-id", 0, "Instance Id")
  val tableFlag = flag("count-table", "count.v1", "Name of the output table")
  val topicFlag = flag("topic", "words-test.v1", "Name of the input topic")
  val appIdFlag = flag("app-id", "word-count.v1", "Application Id")

  def main() {
    val settings = instanceSettings(instanceIdFlag(), appIdFlag())
    val streams = stringCount(settings, topicFlag(), tableFlag())
    streams.start()

    val host = settings.get(StreamsConfig.APPLICATION_SERVER_CONFIG).asInstanceOf[String]

    val server = Http.serve(host, new KeyService(streams, tableFlag(), host))

    sys.addShutdownHook {
      println(s"Closing down kafka streams instance ${instanceIdFlag()}")
      server.close()
      streams.close
    }

    Await.ready(server)
  }
}

