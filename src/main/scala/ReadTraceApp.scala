package org.mkhq.kafka

import com.twitter.app.App

import util.{Failure, Success}

object ReadTraceApp
extends App {
  import Topology._

  val instanceIdFlag = flag("instance-id", 0, "Instance Id")
  val tableFlag = flag("table", "count.v1", "Name of the output table")
  val topicFlag = flag("topic", "words-test.v1", "Name of the input topic")
  val appIdFlag = flag("app-id", "string-count-cli.v1", "Application Id")

  def main() {
    val settings = instanceSettings(instanceIdFlag(), appIdFlag())
    val streams = stringCount(settings, topicFlag(), tableFlag())

    streams.start()

    var reading = true
    while(reading) {
      val key = io.StdIn.readLine("> ")

      if(key == null)
        reading = false
      else {
        streams.read(tableFlag(), key) match {
          case Failure(exc) => println(s"Failed to read key $key, $exc")
          case Success(value) => println(s"$key -> $value")
        }
      }
    }

    streams.close
  }

}


