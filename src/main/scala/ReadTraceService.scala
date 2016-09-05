package org.mkhq.kafka

import com.twitter.finagle.{ChannelException, Http, Service}
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.server.TwitterServer
import com.twitter.util.{Await, Future}

import org.apache.kafka.streams.StreamsConfig

import java.util.Properties
import util.{Failure, Success}

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

    val server = Http.serve(host, Service.mk[Request, Response] { req =>
      val key = req.uri.replace("/","")

      val resp = streams.read(tableFlag(), key) match {
        case Failure(error: KeyNotFound) =>
          val resp = Response(Status.NotFound)
          resp.host = host
          resp

        case Failure(error: StoreUnavailable) =>
          val resp = Response(Status.ServiceUnavailable)
          resp.host = host
          resp

        case Failure(error: Throwable) =>
          val resp = Response(Status.InternalServerError)
          resp.host = host
          resp

        case Success(value) =>
          val resp = Response(Status.Ok)
          resp.contentString = value.toString
          resp.host = host
          resp
      }

      Future.value(resp)
    })

    sys.addShutdownHook {
      println(s"Closing down kafka streams instance ${instanceIdFlag()}")
      server.close()
      streams.close
    }

    Await.ready(server)
  }
}

