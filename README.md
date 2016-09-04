# Kafka Streams Queryable State semantics testing

This repository contain tools to help test the semantics of queryable state in
Kafka Streams. The exact semantics for reading from a queryable state store is
not defined here. Instead we provide tools that generate traces for different
configuration and failure scenarios in a Kafka Streams deployment.

## Build Instructions

### Initialize gradle

This assumes gradle is available on the host system.

```
gradle
```

### Kafka Streams dependencies

Kafka Streams with Queryable State is not released yet so the necessary jars
are included in the lib-dir.

```

kafka-clients-0.10.1.0-SNAPSHOT.jar
kafka-streams-0.10.1.0-SNAPSHOT.jar
kafka_2.11-0.10.1.0-SNAPSHOT.jar
```

### Build and copy the dependencies

This builds the source and copies all dependencies to `build/deps`.

```
./gradlew prepareRun
```

## Running the tools

There are currently tools for testing two scenarios: 1) reading a single key
from a table and 2) reading a single key from one or more instances using an
HTTP client connecting to an HTTP server on each instance. Both scenarios are
setup with a basic topology that counts the unique strings consumed from a
single topic.

## Preparing input data

Create a new topic with the configuration to test using the Kafka CLI tools,
e.g. this topic has 4 partitions on one broker.

```
bin/kafka-topics.sh --zookeeper localhost:2181 --topic strings-test --create --partitions 4 --replication-factor 1
```

Use the console producer to write strings to the topic partitions.

```
bin/kafka-console-producer.sh --broker localhost:9092 --topic strings-test
```

### Single Read Trace App

This app takes keys from stdin and executes a read operation against a store.
The example below reads the key `hello` every second from the store.

```
while true; do echo "hello"; sleep 1; done | java -cp build/deps/*:build/libs/kafka-qs-semantics.jar org.mkhq.kafka.ReadTraceApp -instance-id 0 -topic strings-test -app-id str-app-test
```

### Client/Server App

The client and server apps are used to test more complex scenarios. In this
case we can start multiple server instances which are having unique queryable
state stores. The client is then used to query all instances concurrently
resulting in a trace of the expected value a client would return.

The client takes a key from stdin and executes a read operation on all
configured instances. In the example below we query two instances every second.
The output format is comma separated and has the following fields: request id,
host, http response code, returned value.

```
while true; do echo "hello"; sleep 1; done | java -cp build/deps/*:build/libs/kafka-qs-semantics.jar org.mkhq.kafka.ReadTraceClient -services localhost:2030,localhost:2031
```

Start the first instance

```
java -cp build/deps/*:build/libs/kafka-qs-semantics.jar org.mkhq.kafkReadTraceService -instance-id 0 -admin.port=:9990
```

Start the second instance

```
java -cp build/deps/*:build/libs/kafka-qs-semantics.jar org.mkhq.kafkReadTraceService -instance-id 1 -admin.port=:9991
```

