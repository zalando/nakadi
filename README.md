[![Build Status](https://travis-ci.org/zalando/nakadi.svg)](https://travis-ci.org/zalando/nakadi)
[![ReviewNinja](https://app.review.ninja/44234368/badge)](https://app.review.ninja/zalando/nakadi)
[![codecov.io](https://codecov.io/github/zalando/nakadi/coverage.svg?branch=nakadi-jvm)](https://codecov.io/github/zalando/nakadi?branch=nakadi-jvm)

[![Swagger API](http://online.swagger.io/validator?url=https://raw.githubusercontent.com/zalando/nakadi/nakadi-jvm/api/nakadi-event-bus-api.yaml)](http://online.swagger.io/validator?url=https://raw.githubusercontent.com/zalando/nakadi/nakadi-jvm/api/nakadi-event-bus-api.yaml)

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  

- [Nakadi Event Broker](#nakadi-event-broker)
- [Quickstart](#quickstart)
  - [Running a Server](#running-a-server)
  - [Stopping a Server](#stopping-a-server)
  - [Mac OS Docker Settings](#mac-os-docker-settings)
- [API Overview and Usage](#api-overview-and-usage)
  - [Events and Event Types](#events-and-event-types)
  - [Creating Event Types](#creating-event-types)
    - [Create an Event Type](#create-an-event-type)
    - [List Event Types](#list-event-types)
    - [View an Event Type](#view-an-event-type)
    - [List Partitions for an Event Type](#list-partitions-for-an-event-type)
    - [View a Partition for an Event Type](#view-a-partition-for-an-event-type)
  - [Publishing Events](#publishing-events)
    - [Posting one or more Events](#posting-one-or-more-events)
  - [Consuming Events](#consuming-events)
    - [Opening an Event Stream](#opening-an-event-stream)
    - [Event Stream Structure](#event-stream-structure)
    - [Cursors, Offsets and Partitions](#cursors-offsets-and-partitions)
    - [Event Stream Keepalives](#event-stream-keepalives)
- [Build and Development](#build-and-development)
  - [Building](#building)
  - [Dependencies](#dependencies)
  - [What does the project already implement?](#what-does-the-project-already-implement)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Nakadi Event Broker

The goal of the Nakadi project (ნაკადი means "stream" in Georgian) is 
to build an event broker infrastructure to:

*  Allow event exchange via a RESTful [API](/api/nakadi-event-bus-api.yaml)
*  Enable convenient development of event-driven applications
*  Securely and efficiently publish and consume events

Some additional requirements for the project are to provide:

* Event ordering guarantees
* Low latency (near real-time) event processing
* Scalable and highly available architecture
* Compatability with the [STUPS project](https://stups.io/)

Additional topics, that we plan to cover are:

* Discoverability of the resource structures flowing into the event broker
* A schema registry, to collect schema information for easy lookup by developers
* A managed API that allows consumers to subscribe and have stream offsets stored by the server

## Quickstart

You can run the project locally using [Docker](https://www.docker.com/).

### Running a Server

From the project's home directory you can install and start a Nakadi container via Gradle:

```sh
./gradlew startDockerContainer
```

This will start a docker container for the Nakadi server and another container 
with its PostgreSQL, Kafka and Zookeeper dependencies. The ports 8080 (Nakadi), 
5432 (PostgreSQL), 9092 (Kafka) and 2181 (Zookeeper) are to allow the services 
to communicate with each other and must not be used by other applications.

### Stopping a Server

To stop the running Nakadi:

```sh
./gradlew stopAndRemoveDockerContainer
```

### Mac OS Docker Settings 

Since Docker for Mac OS runs inside Virtual Box, you will  want to expose 
some ports first to allow Nakadi to access its dependencies -

```sh
docker-machine ssh default \
-L 9092:localhost:9092 \
-L 8080:localhost:8080 \
-L 5432:localhost:5432 \
-L 2181:localhost:2181
```

Alternatively you can set up port forwarding on the "default" machine through 
its network settings in the VirtualBox UI. If you get the message "Is the 
docker daemon running on this host?" but you know Docker and VirtualBox are 
running, you might want to run this command - 

```sh
eval "$(docker-machine env default)"
```


## API Overview and Usage

### Events and Event Types

The Nakadi API allows the publishing and consuming of _events_ over HTTP. 
To do this the producer must register an _event type_ with the Nakadi schema 
registry. 

The event type contains information such as the name, the owning application, 
strategies for partitioning and enriching data, and a JSON  schema. Once the 
event type is created, a publishing resource becomes available that will accept 
events for the type, and consumers can also read from the event stream.

There are three main _categories_ of event type defined by Nakadi - 

- Undefined: A free form category suitable for events that are entirely custom to the producer.

- Data: an event that represents a change to a record or other item, or a new item. Change events are associated with a create, update, delete, or snapshot operation. 

- Business: an event that is part of, or drives a business process, such as a state transition in a customer order. 

The events for the business and data change helper categories follow a 
generic Nakadi event schema as well as a schema custom to the event data. The generic 
schema pre-defines common fields for an event and the custom schema for the event 
is defined when the event type is created. When a JSON event for one of these 
categories is posted to the server, it is expected to conform to the 
combination of the generic schema for the category and to the custom schema defined 
for the event type. This combination is called the _effective schema_ and is 
validated by Nakadi.

The undefined category is also required to have a JSON schema on creation, 
but this can be as simple as `{ "\additionalProperties\": true }` to allow arbitrary 
JSON. Unlike the business and data categories, the schema for an undefined type is 
not checked by Nakadi when an event is posted, but it can be used by a consumer 
to validate data on the stream.

### Creating Event Types

#### Create an Event Type 

An event type can be created by posting to the `event-types` resource. 

Each event type must have a unique `name`. If the event type already exists a 
`409 Conflict` response will be returned. Otherwise a successful request will 
result in a `201 Created` response. The exact required fields depend on the 
event type's category, but `name`, `owning_application` and `schema` are always 
expected.

The `schema` value should only declare the custom part of the event - the generic 
schema is implicit and doesn't need to be defined. The combination of the two 
(the "effective schema") will be checked when events are submitted for the event type.

This example shows a `business` category event type with a simple schema for an 
order number -

```sh
curl -v -XPOST http://localhost:8080/event-types -d '{
  "name": "order.ORDER_RECEIVED",
  "owning_application": "order-service",
  "category": "business",
  "partition_strategy": "random",
  "enrichment_strategies": ["metadata_enrichment"],
  "schema": {
    "type": "json_schema",
    "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }"
  }
}'
```

This example shows an `undefined` category event type with a wilcard schema -

```sh
curl -v -XPOST http://localhost:8080/event-types -d '{
  "name": "undef",
  "owning_application": "jinteki",
  "category": "undefined",
  "partition_strategy": "random",
  "schema": {
    "type": "json_schema",
    "schema": "{ \"additionalProperties\": true }"
  }
}'
```

An undefined event does not accept a value for `enrichment_strategies`.

#### List Event Types

```sh
curl -v http://localhost:8080/event-types


HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8

[
  {
    "category": "business",
    "default_statistic": null,
    "enrichment_strategies": ["metadata_enrichment"],
    "name": "order.ORDER_RECEIVED",
    "owning_application": "order-service",
    "partition_key_fields": [],
    "partition_strategy": "random",
    "schema": {
      "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }",
      "type": "json_schema"
    }
  }
]
```

#### View an Event Type

Each event type registered with Nakadi has a URI based on its `name` -

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED


HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8

{
  "category": "business",
  "default_statistic": null,
  "enrichment_strategies": ["metadata_enrichment"],
  "name": "order.ORDER_RECEIVED",
  "owning_application": "order-service",
  "partition_key_fields": [],
  "partition_strategy": "random",
  "schema": {
    "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }",
    "type": "json_schema"
  }
}
```

#### List Partitions for an Event Type

The partitions for an event type are available via its `/partitions` resource:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/partitions 


HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8

[
  {
    "newest_available_offset": "BEGIN",
    "oldest_available_offset": "0",
    "partition": "0"
  }
]
```

#### View a Partition for an Event Type

Each partition for an event type has a URI based on its `partition` value:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/partitions/0 


HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8

{
  "newest_available_offset": "BEGIN",
  "oldest_available_offset": "0",
  "partition": "0"
}
```

### Publishing Events

#### Posting one or more Events

Events for an event type can be published by posting to its "events" collection:

```sh
curl -v -XPOST http://localhost:8080/event-types/order.ORDER_RECEIVED/events -d '[
  {
    "order_number": "24873243241",
    "metadata": {
      "eid": "d765de34-09c0-4bbb-8b1e-7160a33a0791",
      "occurred_at": "2016-03-15T23:47:15+01:00"
    }
  }, {
    "order_number": "24873243242",
    "metadata": {
      "eid": "a7671c51-49d1-48e6-bb03-b50dcf14f3d3",
      "occurred_at": "2016-03-15T23:47:16+01:00"
    }
  }]'


HTTP/1.1 200 OK  
```

The events collection accepts an array of events. As well as the fields defined 
in the event type's schema, the posted event must also contain a `metadata` 
object with an `eid` and `occurred_at` fields. The `eid` is a UUID that uniquely 
identifies an event and the `occurred_at` field identifies the time of creation 
of the Event defined by the producer.

Note that the order of events in the posted array will be the order they are published 
onto the event stream and seen by consumers. They are not re-ordered based on 
their `occurred_at` or other data values. 

### Consuming Events

#### Opening an Event Stream

You can open a stream for an Event Type via the `events` sub-resource:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/events 
```

#### Event Stream Structure

The stream response groups events into batches. Batches in the response 
are separated by a newline and each batch will be emitted on a single 
line, but a pretty-printed batch object looks like this -

```json
{
  "cursor": {
    "partition": "0",
    "offset": "4"
  },
  "events": [{
    "order_number": "24873243241",
    "metadata": {
      "eid": "d765de34-09c0-4bbb-8b1e-7160a33a0791",
      "occurred_at": "2016-03-15T23:47:15+01:00"
    }
  }, {
    "order_number": "24873243242",
    "metadata": {
      "eid": "a7671c51-49d1-48e6-bb03-b50dcf14f3d3",
      "occurred_at": "2016-03-15T23:47:16+01:00"
    }
  }]
} 
```

The `cursor` object describes the partition and the offset for this batch of 
events. The cursor allow clients to checkpoint which events have already been 
consumed and navigate through the stream - individual events in the stream don't 
have cursors. The `events` array contains a list of events that were published in 
the order they were posted by the producer. Each event will contain a `metadata` 
field as well as the custom data defined by the event type's schema.

The HTTP response then will look something like this -

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/events 
    

HTTP/1.1 200 OK

{"cursor":{"partition":"0","offset":"4"},"events":[{"order_number": "ORDER_001", "metadata": {"eid": "4ae5011e-eb01-11e5-8b4a-1c6f65464fc6", "occurred_at": "2016-03-15T23:56:11+01:00"}}]}
{"cursor":{"partition":"0","offset":"5"},"events":[{"order_number": "ORDER_002", "metadata": {"eid": "4bea74a4-eb01-11e5-9efa-1c6f65464fc6", "occurred_at": "2016-03-15T23:57:15+01:00"}}]}
{"cursor":{"partition":"0","offset":"6"},"events":[{"order_number": "ORDER_003", "metadata": {"eid": "4cc6d2f0-eb01-11e5-b606-1c6f65464fc6", "occurred_at": "2016-03-15T23:58:15+01:00"}}]}
```

#### Cursors, Offsets and Partitions

By default the `events` resource will consume from all partitions of an event 
type and from the end (or "tail") of the stream. To select only particular 
partitions and a position where in the stream to start, you can supply 
an `X-Nakadi-Cursors` header in the request:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/events \
  -H 'X-Nakadi-Cursors: [{"partition": "0", "offset":"12"}]'
```

The header value is a JSON array of _cursors_. Each cursor in the array 
describes its partition for the stream and an offset to stream from. Note that 
events within the same partition maintain their overall order.

The `offset` value of the cursor allows you select where the in the stream you 
want to consume from. This can be any known offset value, or the dedicated value 
`BEGIN` which will start the stream from the beginning. For example, to read 
from partition `0` from the beginning:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/events \
  -H 'X-Nakadi-Cursors:[{"partition": "0", "offset":"BEGIN"}]'
```

The details of the partitions and their offsets for an event type are 
available via its `partitions` resource.

#### Event Stream Keepalives

If there are no events to be delivered Nakadi will keep a streaming connection open by 
periodically sending a batch with no events but which contains a `cursor` pointing to 
the current offset. For example:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/events 
      

HTTP/1.1 200 OK

{"cursor":{"partition":"0","offset":"6"},"events":[{"order_number": "ORDER_003", "metadata": {"eid": "4cc6d2f0-eb01-11e5-b606-1c6f65464fc6", "occurred_at": "2016-03-15T23:58:15+01:00"}}]}
{"cursor":{"partition":"0","offset":"6"}}
{"cursor":{"partition":"0","offset":"6"}}
{"cursor":{"partition":"0","offset":"6"}}
{"cursor":{"partition":"0","offset":"6"}}
```

This can be treated as a keep-alive control for some load balancers.

## Build and Development

### Building

The project is built with [Gradle](http://gradle.org). The `./gradlew` 
[wrapper script](http://www.gradle.org/docs/current/userguide/gradle_wrapper.html) will bootstrap the right Gradle version if it's not already installed. 

The gradle setup is fairly standard, the main tasks are:

- `./gradlew build`: run a build and test
- `./gradlew clean`: clean down the build

Some other useful tasks are:

- `./gradlew acceptanceTest`: run the ATs
- `./gradlew fullAcceptanceTest`: run the ATs in the context of Docker
- `./gradlew dbBootstrap`: set up the database
- `./gradlew cleanDb`: clear down the database
- `./gradlew startDockerContainer`: start the docker containers (and download images if needed)
- `./gradlew stopAndRemoveDockerContainer`: shutdown the docker processes
- `./gradlew startStoragesInDocker`: start the storage container (handy for running Nakadi directly or in your IDE)
- `./gradlew buildFullDockerImage`: creates a single docker image containing all relevant components to run it standalone. Find more information in `full-docker-image/README.md`.


For working with an IDE, the `eclipse` IDE task is available and you'll be able to import the `build.gradle` into Intellij IDEA directly.

### Dependencies

The Nakadi server is a Java 8 [Spring Boot](http://projects.spring.io/spring-boot/) application. It uses [Kafka 0.9](http://kafka.apache.org/090/documentation.html) as its broker and [PostgreSQL 9.5](http://www.postgresql.org/docs/9.5/static/release-9-5.html) as its supporting database.

### What does the project already implement?

* [x] REST abstraction over Kafka-like queues
* [ ] Support of event filtering per subscriptions
* [x] creation of event types
* [x] low-level interface
    * manual client side partition management is needed
    * no support of commits
* [ ] high-level interface
    * automatic redistribution of partitions between consuming clients
    * commits should be issued to move server-side cursors
