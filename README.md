[![Build Status](https://travis-ci.org/zalando/nakadi.svg)](https://travis-ci.org/zalando/nakadi)
[![ReviewNinja](https://app.review.ninja/44234368/badge)](https://app.review.ninja/zalando/nakadi)
[![codecov.io](https://codecov.io/github/zalando/nakadi/coverage.svg?branch=nakadi-jvm)](https://codecov.io/github/zalando/nakadi?branch=nakadi-jvm)

[![Swagger API](http://online.swagger.io/validator?url=https://raw.githubusercontent.com/zalando/nakadi/nakadi-jvm/api/nakadi-event-bus-api.yaml)](http://online.swagger.io/validator?url=https://raw.githubusercontent.com/zalando/nakadi/nakadi-jvm/api/nakadi-event-bus-api.yaml)

## Nakadi Event Bus

The goal of the `nakadi` project (ნაკადი means `stream` in Georgian language) is to build an event bus infrastructure to:

*  enable convenient development of event-driven applications
*  securely and efficiently publish and consume events as easy as possible
*  abstract event exchange by a standardized RESTful [API](/api/nakadi-event-bus-api.yaml)

Some additional technical requirements that we wanted to cover by this architecture:

* event ordering guarantees
* fast (near real-time) event processing
* scalable and highly available architecture
* [STUPS](https://stups.io/) compatible

Additional topics, that we plan to cover in the near future are:

* discoverability of the resource structures flowing into the event bus
* centralized discovery service, that will use these capabilities to collect resource schema information for easy lookup by developers

> NOTE: it is not really clear if the resource schema discoverability service should be part of `nakadi` event bus

### What does the project already implement?

* [x] REST abstraction over Kafka-like queues
* [ ] support of event filtering per subscriptions
* streaming/batching of events to/from the clients
  * [x] creation of topics
  * [x] low-level interface
    * manual client side partition management is needed
    * no support of commits
  * [ ] high-level interface
    * automatic redistribution of partitions between consuming clients
    * commits should be issued to move server-side cursors

## Running it locally

### To run the project locally

Simple Nakadi startup:

```sh
./gradlew startDockerContainer
```

It will start a docker container with all dependencies and another docker container running Nakadi itself. Please be
aware that the ports 8080 (Nakadi), 5432 (PostgreSQL), 9092 (Kafka) and 2181 (Zookeeper) are needed and must not be
blocked by another application.

To stop the running Nakadi again:

```sh
./gradlew stopAndRemoveDockerContainer
```

### Full development pipeline:

    build -> ut/it tests (depends on access to a Kafka backend) -> docker (builds docker image) -> api-tests (runs tests against the docker image)

## Usage

### Create new event type (business event)

```sh
curl --request POST \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types -d '{
  "name": "order.ORDER_RECEIVED",
  "owning_application": "order-service",
  "category": "business",
  "partition_key_fields": [],
  "schema": {
    "type": "json_schema",
    "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }"
  }
}'
```

### Get existing event types

```sh
curl --request GET \
     http://localhost:8080/event-types
```

### Get event type schema

```sh
curl --request GET \
     http://localhost:8080/event-types/order.ORDER_RECEIVED
```

### Get all partitions for event type

```sh
curl --request GET \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types/order.ORDER_RECEIVED/partitions
```

### Get single partition for event type

```sh
curl --request GET \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types/order.ORDER_RECEIVED/partitions/0
```

### Publish events

```sh
curl --request POST \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types/order.ORDER_RECEIVED/events \
     -d '[
       { "order_number": "ORDER_ONE", "metadata": { "eid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "occurred_at": "2016-03-15T23:47:15+01:00" } },
       { "order_number": "ORDER_TWO", "metadata": { "eid": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "occurred_at": "2016-03-15T23:47:16+01:00" } }
     ]'
```

### Receive event stream

You can consume from all partitions of an event type by just getting the `events` sub-resource:

```sh
curl --request GET \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types/order.ORDER_RECEIVED/events
```

You can consume only certain partitions or from in the middle of the stream or from the beginning by using the `X-Nakadi-Cursors` header.
Lookup the partition cursors with `/event-types/order.ORDER_RECEIVED/partitions` in order to provide a valid `X-Nakadi-Cursors` header.

For instance, to read from partition `0` from the beginning:

```sh
curl --request GET \
     --header "Content-Type:application/json" \
     --header 'X-Nakadi-Cursors:[{"partition": "0", "offset":"BEGIN"}]' \
     http://localhost:8080/event-types/order.ORDER_RECEIVED/events
```

The stream contains events together with the cursors, so that clients can remember which events have already been consumed and navigate through the stream. Example:

```sh
$ curl --request GET \
       --header "Content-Type:application/json" \
       --header 'X-Nakadi-Cursors:[{"partition": "0", "offset":"3"}]' \
       http://localhost:8080/event-types/order.ORDER_RECEIVED/events
{"cursor":{"partition":"0","offset":"4"},"events":[{"order_number": "ORDER_001", "metadata": {"eid": "4ae5011e-eb01-11e5-8b4a-1c6f65464fc6", "occurred_at": "2016-03-15T23:56:11+01:00"}}]}
{"cursor":{"partition":"0","offset":"5"},"events":[{"order_number": "ORDER_002", "metadata": {"eid": "4bea74a4-eb01-11e5-9efa-1c6f65464fc6", "occurred_at": "2016-03-15T23:57:15+01:00"}}]}
{"cursor":{"partition":"0","offset":"6"},"events":[{"order_number": "ORDER_003", "metadata": {"eid": "4cc6d2f0-eb01-11e5-b606-1c6f65464fc6", "occurred_at": "2016-03-15T23:58:15+01:00"}}]}
{"cursor":{"partition":"0","offset":"6"}}
```

Note that the offset is zero based and exclusive.

blah