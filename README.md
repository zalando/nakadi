[![Build Status](https://travis-ci.org/zalando/nakadi.svg)](https://travis-ci.org/zalando/nakadi)
[![codecov.io](https://codecov.io/github/zalando/nakadi/coverage.svg?branch=nakadi-jvm)](https://codecov.io/github/zalando/nakadi?branch=nakadi-jvm)

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
- [Contributing](#contributing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Nakadi Event Broker

The goal of Nakadi (ნაკადი means "stream" in Georgian) is to provide an event broker infrastructure to:

- Abstract event delivery via a secured [RESTful API](api/nakadi-event-bus-api.yaml). This allows microservices teams to maintain service boundaries, and not directly depend on any specific message broker technology. Access to the API can be managed and secured using OAuth scopes.

- Enable convenient development of event-driven applications and asynchronous microservices. Event types can be defined with schemas and managed via a registry. Nakadi also has optional support for events describing business processes and data changes using standard primitives for identity, timestamps, event types, and causality. 

-  Efficient low latency event delivery. Once a publisher sends an event using a simple HTTP POST, consumers can be pushed to via a streaming HTTP connection, allowing near real-time event processing. The consumer connection has keepalive controls and support for managing stream offsets. 

The project also provides compatability with the [STUPS project](https://stups.io/). Additional features that we plan to cover in the future are:

* Discoverability of the resource structures flowing into the broker.

* A managed API that allows consumers to subscribe and have stream offsets stored by the server.

* Filtering of events for subscribing consumers.

* Role base access control to data. 

* Support for different streaming technologies and engines. Nakadi currently uses [Apache Kafka](http://kafka.apache.org/) as its broker, but other providers (such as Kinesis) will be possible. 

More detailed information can be found on the [manual](http://zalando.github.io/nakadi-manual/). 


## Quickstart

You can run the project locally using [Docker](https://www.docker.com/). Note that Nakadi requires very recent versions of docker and docker-compose. See [Dependencies](#dependencies) for more information.

### Running a Server

From the project's home directory you can start Nakadi via Gradle:

```sh
./gradlew startNakadi
```

This will build the project and run docker compose with 4 services:

- Nakadi (8080)
- PostgreSQL (5432)
- Kafka (9092)
- Zookeeper (2181)

### Stopping a Server

To stop the running Nakadi:

```sh
./gradlew stopNakadi
```

### Mac OS Docker Settings

Since Docker for Mac OS runs inside Virtual Box, you will  want to expose
some ports first to allow Nakadi to access its dependencies:

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
running, you might want to run this command:

```sh
eval "$(docker-machine env default)"
```
Note: Docker for Mac OS (previously in beta) version 1.12 (1.12.0 or 1.12.1) currently is not supported due to the [bug](https://github.com/docker/docker/issues/22753#issuecomment-242711639) in networking host configuration.

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

## Subscription API

### Subscriptions
Using subscriptions users are able to consume events from event-types in a "high level" way when 
Nakadi stores the offsets and manages the rebalancing of consuming clients. The clients using 
subscriptions can stay really stateless. It is possible to subscribe to multiple event-types in 
one subscription* so that within one connection it is possible to read events from many event-types.

_(\* This possibility will be enabled soon)_

The typical workflow when using subscriptions is following:

1) create subscription specifying the event-types you want to read;

2) start reading events from your subscription;

3) commit the cursors you get in event batches;

If you closed the connection and after some time started reading again - you get events from the 
point of your latest commit.
Also, it is not necessary to commit each batch you get. When you commit the cursor, all events that
are before this cursor will also be considered committed.

If you need more that one client for your subscription to distribute the load - you can start 
reading the subscription with multiple clients and Nakadi will balance the load among your clients. 
The balancing units are partitions, so the number of clients of your subscription can't be higher 
than the total number of all partitions of the event-types of your subscription. 

_E.g. if you have a subscription for two event-types A and B having 2 and 4 partitions accordingly
then if you start reading events by a single client - you will get events from all 6 partitions
by this client. If you connect by a second client - 3 partitions will be transferred from first client
to a second client and each client will be getting data from 3 partitions. So the maximum possible 
number of clients for this subscription is 6 (in that case each one will read from one subscription)_

Bellow please find basic examples of Subscription API usage. For more detailed description and advanced
configuration please take a look at Nakadi [swagger](api/nakadi-event-bus-api.yaml) file.

### Creating subscriptions
The subscription can be created by posting to `/subscriptions`resourse.
```sh
curl -v -X POST -H "Content-type: application/json" \
  "http://localhost:8080/subscriptions" -d '{
    "owning_application": "order-service",
    "event_types": ["order.ORDER_RECEIVED"]
  }'
    
```
In response you will get the whole subscription object that was created including the id.
```sh
HTTP/1.1 201 Created
Content-Type: application/json;charset=UTF-8
{
  "owning_application": "order-service",
  "event_types": [
    "order.ORDER_RECEIVED"
  ],
  "consumer_group": "default",
  "read_from": "end",
  "id": "038fc871-1d2c-4e2e-aa29-1579e8f2e71f",
  "created_at": "2016-09-23T16:35:13.273Z"
}
```

### Reading events
Reading events is possible by sending GET request to `/subscriptions/{subscription-id}/events` endpoint 
```sh
curl -v -X GET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/events"
```
The response is a stream that groups events into JSON batches separated by endline character.

The output looks like this:
```sh
{"cursor":{"partition":"5","offset":"543","event_type":"order.ORDER_RECEIVED","cursor_token":"b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"},"info":{"debug":"Stream started"}]}
{"cursor":{"partition":"5","offset":"544","event_type":"order.ORDER_RECEIVED","cursor_token":"a28568a9-1ca0-4d9f-b519-dd6dd4b7a610"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"}]}
{"cursor":{"partition":"5","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"a241c147-c186-49ad-a96e-f1e8566de738"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"}]}
{"cursor":{"partition":"0","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"bf6ee7a9-0fe5-4946-b6d6-30895baf0599"}}
{"cursor":{"partition":"1","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"9ed8058a-95be-4611-a33d-f862d6dc4af5"}}
```
Each batch contains the following fields:

- `cursor`: the cursor of the batch which should be used for committing the batch;
- `events`: the array of events of this batch;
- `info`: optional field that can hold some useful information (e.g. the reason why the stream was closed by Nakadi)

### Subscription cursors
In Subscription API cursors have the following structure:
```sh
{
  "partition": "5",
  "offset": "543",
  "event_type": "order.ORDER_RECEIVED",
  "cursor_token": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
}
```
Fields are:

- `partition`: the partition this batch comes from;
- `offset`: the offset of this batch; user should not do any operations with offset, for him it should be just a string;
- `event_type`: specifies the event-type of the cursor (as in one stream there can be events of different event-types);
- `cursor_token`: cursor token generated by Nakadi; useless for the user;

### Committing cursors
```sh
curl -v -X POST \
    -H "Content-type: application/json" \
    -H "X-Nakadi-StreamId: ae1e39c3-219d-49a9-b444-777b4b03e84c" \    
    "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/cursors" \
    -d '{
      "items": [
        {
          "partition": "5",
          "offset": "543",
          "event_type": "order.ORDER_RECEIVED",
          "cursor_token": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
        },
        {
          "partition": "2",
          "offset": "923",
          "event_type": "order.ORDER_RECEIVED",
          "cursor_token": "a28568a9-1ca0-4d9f-b519-dd6dd4b7a610"
        }
      ]
    }'
```

### Checking currently committed cursors
To see what is current position of subscription it's possible to run the request:
```sh
curl -v -X GET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/cursors"
```
The response will be a list of current cursors that reflect the last committed offsets.

### Rebalance

### Subscription statistics
To get statistics of subscription the folowing request should be used:
```sh
curl -v -X GET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/stats"
```

The output will contain the statistics for all partitions of the stream. Like this:
```sh
{
  "items": [
    {
      "event_type": "order.ORDER_RECEIVED",
      "partitions": [
        {
          "partition": "0",
          "state": "reassigning",
          "unconsumed_events": 2115,
          "client_id": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
        },
        {
          "partition": "1",
          "state": "assigned",
          "unconsumed_events": 1029,
          "client_id": "ae1e39c3-219d-49a9-b444-777b4b03e84c"
        }
      ]
    }
  ]
}
```

### Deleting subscription
```sh
curl -v -X DELETE "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f"
```

### Getting/listing subscriptions
```sh
curl -v -X GET "http://localhost:8080/subscriptions"
```

It's possible to filter the list with following parameters: `event_type`, `owning_application`. 
Also, pagination parameters are available: `offset`, `limit`.

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
- `./gradlew startNakadi`: build Nakadi and start docker-compose services: nakadi, postgresql, zookeeper and kafka
- `./gradlew stopNakadi`: shutdown docker-compose services
- `./gradlew startStorages`: start docker-compose services: postgres, zookeeper and kafka (useful for development purposes)
- `./gradlew stopStorages`: shutdown docker-compose services

For working with an IDE, the `eclipse` IDE task is available and you'll be able to import the `build.gradle` into Intellij IDEA directly.

### Dependencies

The Nakadi server is a Java 8 [Spring Boot](http://projects.spring.io/spring-boot/) application. It uses [Kafka 0.9](http://kafka.apache.org/090/documentation.html) as its broker and [PostgreSQL 9.5](http://www.postgresql.org/docs/9.5/static/release-9-5.html) as its supporting database.

Nakadi requires recent versions of docker and docker-compose. In
particular, docker-compose >= v1.7.0 is required. See [Install Docker
Compose](https://docs.docker.com/compose/install/) for information on
installing the most recent docker-compose version.

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

## Contributing

Nakadi accepts contributions from the open-source community. Please see the [issue tracker](https://github.com/zalando/nakadi/issues) for things to work on.

Before making a contribution, please let us know by posting a comment to the relevant issue. And if you would like to propose a new feature, do start a new issue explaining the feature you’d like to contribute.

