package org.mkhq.kafka

import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state._

import util.{Try, Success, Failure}

object Topology {

  object Errors {
    trait ReadError extends Throwable
    case class StoreUnavailable(table: String) extends ReadError
    case class StoreUnknownError(table: String, error: Throwable) extends ReadError
    case class KeyNotFound(table: String, key: String) extends ReadError
    case class KeyAtOtherStore(location: String) extends ReadError
  }

  def instanceSettings(id: Int, app: String, offsetReset: String = "earliest"): Properties = {
    val port = s"203$id".toInt
    val host = s"localhost:$port"

    val stateDir = s"/tmp/kafka-qs-read-trace/$app/$id/"

    Files.createDirectories(Paths.get(stateDir))

    val settings = new Properties()
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset)
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, app)
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
    settings.put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
    settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host)

    settings
  }

  implicit class KafkaStreamsPlus(streams: KafkaStreams) {
    import Errors._

    def verifyKeyAtHost(table: String, key: String, host: String): Try[Unit] = {
      // verify that this host should store the key
      val hostInfo = streams.metadataForKey(table, key, Serdes.String.serializer)
      val newHost = s"${hostInfo.host}:${hostInfo.port}"

      if(newHost == host) Success(())
      else Failure(KeyAtOtherStore(newHost))
    }

    def read(table: String, key: String): Try[Long] = {
      // NOTE: this cannot be a scala Long since it will return 0 instead of null
      Try(streams.store(table, QueryableStoreTypes.keyValueStore[String, java.lang.Long])) match {
        case Failure(isse: InvalidStateStoreException) =>
          Failure(StoreUnavailable(table))

        case Failure(exc: Throwable) =>
          Failure(StoreUnknownError(table, exc))

        case Success(null) =>
          Failure(StoreUnavailable(table))

        case Success(store) =>
          store.get(key) match {
            case null => Failure(KeyNotFound(table, key))
            case value => Success(value)
        }
      }
    }
  }

  def stringCount(settings: Properties, topic: String, table: String) = {
    val builder: KStreamBuilder = new KStreamBuilder
    val stream = builder.stream(Serdes.String, Serdes.String, topic)

    stream
      .map((_, value) => new KeyValue(value.toLowerCase, value))
      .groupByKey
      .count(table)

    new KafkaStreams(builder, settings)
  }
}


