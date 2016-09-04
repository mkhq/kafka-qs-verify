package org.mkhq.kafka

import com.twitter.app.App
import com.twitter.finagle.{ChannelException, Http, Service}
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import com.twitter.server.TwitterServer

import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state._

import util.Try

object ReadTraceClient
extends App {
  val servicesFlag = flag("services", Seq[String](), "List of services, <host>:<port>,...")

  def main(): Unit = {
    val services = servicesFlag().map { addr =>
      Http.client
        .withSessionQualifier
        .noFailFast
        .withSessionQualifier
        .noFailureAccrual
        .newService(addr)
    }

    onExit {
      services.map(s => Await.ready(s.close()))
    }

    var reqId = 0
    var reading = true
    while(reading) {
      val key = io.StdIn.readLine

      if(key == null)
        reading = false
      else {
        val req = Request(Method.Get, s"/$key")
        val fs = Future
          .collect(services.map(s => s(req).rescue {
            case exc: ChannelException =>
              val resp = Response(Status.ServiceUnavailable)
              // extract the host
              exc.remoteInfo match {
                case RemoteInfo.Available(_, _, downAddr, _, _) =>
                  resp.host = downAddr.map(_.toString).getOrElse("")
                case _ =>
              }

              Future.value(resp)

            case exc: Exception =>
              println("Unknown error " + exc)
              val resp = Response(Status.ServiceUnavailable)
              Future.value(resp)

          }))
          .map { resps =>
            resps
              .map(r => s"$reqId,${r.host.getOrElse("NA")},${r.statusCode},${r.contentString}")
              .mkString("\n")
          }

        println(Await.result(fs))
      }

      reqId += 1
    }
  }
}


