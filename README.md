[![Build Status](https://travis-ci.org/zalando/nakadi.svg)](https://travis-ci.org/zalando/nakadi)
[![ReviewNinja](https://app.review.ninja/44234368/badge)](https://app.review.ninja/zalando/nakadi)
[![codecov.io](https://codecov.io/github/zalando/nakadi/coverage.svg?branch=nakadi-jvm)](https://codecov.io/github/zalando/nakadi?branch=nakadi-jvm)

[![Swagger API](http://online.swagger.io/validator?url=https://raw.githubusercontent.com/zalando/nakadi/nakadi-jvm/api/nakadi-event-bus-api.yaml)](http://online.swagger.io/validator?url=https://raw.githubusercontent.com/zalando/nakadi/nakadi-jvm/api/nakadi-event-bus-api.yaml)

## Introduction

Nakadi is an *event bus* that allows for applications to easily
publish and consume *streams* of events. It's goal is to enable
convenient development of event-driven applications using simple
RESTful APIs.

Check our [getting started guide](#getting-started) to get up
and running quickly. For a more detailed description of features,
please check the [API specification](api/nakadi-event-bus-api.yaml).

### Main features

* Easy to use *RESTful API* for publishing and consuming events.
* Built-in *schema registry* and event publishing validation.
* OAuth2 authenticaticable.

## Getting Started

### Install locally

Simple Nakadi startup:

```sh
./gradlew startDockerContainer
```

The following command is for Mac OS users only, since bootstrap script
requires access to these ports.

```sh
docker-machine ssh default -L 9092:localhost:9092 -L 8080:localhost:8080 -L 5432:localhost:5432 -L 2181:localhost:2181
```

### Using Nakadi

#### Create new event type

In order to publish messages to Nakadi, users must first create an
`EventType`. In a nutshel, it has the following roles:

1. storing an event schema (for further event validations),
2. configuring throughput options for optimized performance,
3. providing an endpoint for publishing and consuming events.

Example:

```sh
curl --request POST \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types -d '{
  "name": "example_application.greatings",
  "owning_application": "example_application",
  "category": "undefined",
  "schema": {
    "type": "json_schema",
    "schema": "{ \"properties\": { \"message\": { \"type\": \"string\" } } }"
  }
}'
```

Be aware that if you are running Nakadi in `BASIC` or `FULL`
authentication mode `NAKADI_OAUTH2_MODE`, you should also provide a
Bearer token in the header.

#### Listing available event types

If you are interested in subscribing to an existing event type, the
first thing to do is to check the list of available ones.

```sh
curl --request GET \
     http://localhost:8080/event-types
```

In case you already know the name of the event, it's possible to check
for its details directly.

```sh
curl --request GET \
     http://localhost:8080/event-types/example_application.greatings
```

#### Publish events

Now that an `EventType` has been created, we should be able to publish
events to it.

```sh
curl --request POST \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types/example_application.greatings/events \
     -d '[
       { "message": "hello" },
       { "message": "world" }
     ]'
```

#### Partitions

In order to achieve high throughput, Nakadi exposes its internal unit
of storage, called *partition*. A partition is a sequential list of
events. By default event types contain only a single partition. But if
you need extra power, it's possible to configure an `EventType` for
having more than one partition.

```sh
curl --request GET \
     --header "Content-Type:application/json" \
     http://localhost:8080/event-types/order.ORDER_RECEIVED/partitions
```

#### Streamming events

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

## Why use it

At Zalando, Nakadi solves the problem of making distributed
micro-services data available in a single place for business
inteligence to collect, store and analyse it.

It allows for a convenient way of building event-driven applications.

Besides that, Nakadi was designed to achieve 4 important goals:

1. Easy to use

Differently from other queuing systems, Nakadi clients relies on a
simple to use RESTful API for creating streams, producing and
consuming events. We thoroughly document our API using OpenAPI 2.0
specification standard.

2. Safe

Using HTTPS and OAuth2, Nakadi relies on battle tested standards for
authentication and encripted data transfer.

3. High quality data

Having a built-in schema-registry, Nakadi provides a centralized
repository for all available types, encreasing the value of data by
making it discoverable and understandable.

4. Performance

Nakadi is designed so to add minimal overhead per event processed. It
exposes a bulk submission API for optmizing publishing and allow for
stream compression targeting faster delivery.

## Running Nakadi in Production

## License

Nakadi is delivered under [MIT license](LICENSE).
