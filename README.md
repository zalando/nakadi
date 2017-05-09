[![Build Status](https://travis-ci.org/zalando/nakadi.svg?branch=master)](https://travis-ci.org/zalando/nakadi)
[![codecov.io](https://codecov.io/github/zalando/nakadi/coverage.svg?branch=master)](https://codecov.io/github/zalando/nakadi?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/785ccd4ab5e34867b760a8b07c3b62f1)](https://www.codacy.com/app/aruha/nakadi?utm_source=www.github.com&amp;utm_medium=referral&amp;utm_content=zalando/nakadi&amp;utm_campaign=Badge_Grade)

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
  - [Subscriptions](#subscriptions)
    - [Creating Subscriptions](#creating-subscriptions)
    - [Consuming Events from a Subscription](#consuming-events-from-a-subscription)
    - [Client Rebalancing](#client-rebalancing)
    - [Subscription Cursors](#subscription-cursors)
    - [Committing Cursors](#committing-cursors)
    - [Checking Current Position](#checking-current-position)
    - [Subscription Statistics](#subscription-statistics)
    - [Deleting a Subscription](#deleting-a-subscription)
    - [Getting and Listing Subscriptions](#getting-and-listing-subscriptions)
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

Each event type can have a `default_statistic` object attached. It controls the
number of partitions of the underlying topic. If you do not provide any value, 
Nakadi will use a sensible default value which may be just a single partition.
This will effectively disallow parallel reads of subscriptions of this event
type. The values provided here can not be changed later, so choose them wisely. 

This example shows a `business` category event type with a simple schema for an 
order number -

```sh
curl -v -XPOST http://localhost:8080/event-types -H "Content-type: application/json" -d '{
  "name": "order.ORDER_RECEIVED",
  "owning_application": "order-service",
  "category": "business",
  "partition_strategy": "hash",
  "partition_key_fields": ["order_number"],
  "enrichment_strategies": ["metadata_enrichment"],
  "default_statistic": {
    "messages_per_minute": 1000,	
    "message_size":	5,
    "read_parallelism":	1,
    "write_parallelism": 1
  },
  "schema": {
    "type": "json_schema",
    "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }"
  }
}'
```

This example shows an `undefined` category event type with a wilcard schema -

```sh
curl -v -XPOST http://localhost:8080/event-types -H "Content-type: application/json" -d '{
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
    "partition_key_fields": ["order_number"],
    "partition_strategy": "hash",
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
  "partition_key_fields": ["order_number"],
  "partition_strategy": "hash",
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
curl -v -XPOST http://localhost:8080/event-types/order.ORDER_RECEIVED/events -H "Content-type: application/json" -d '[
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

### Subscriptions

Subscriptions allow clients to consume events, where the Nakadi server store offsets and 
automatically manages reblancing of partitions across consumer clients. This allows clients 
to avoid managing stream state locally.

The typical workflow when using subscriptions is:

1. Create a Subscription specifying the event-types you want to read.

1. Start reading batches of events from the subscription. 

1. Commit the cursors found in the event batches back to Nakadi, which will store the offsets. 


If the connection is closed, and later restarted, clients will get events from 
the point of your last cursor commit. If you need more than one client for your 
subscription to distribute the load you can read the subscription with multiple 
clients and Nakadi will balance the load across them.

The following sections provide more detail on the Subscription API and basic 
examples of Subscription API creation and usage:

  - [Creating Subscriptions](#creating-subscriptions): How to create a new Subscription and select the event types.
  - [Consuming Events from a Subscription](#consuming-events-from-a-subscription): How to connect to and consume batches from a Susbcription stream.
  - [Client Rebalancing](#client-rebalancing): Describes how clients for a Subscription are automatically assigned partitions, and how the API's _at-least-once_ delivery guarantee works.
  - [Subscription Cursors](#subscription-cursors): Describes the structure of a Subscription batch cursor.
  - [Committing Cursors](#committing-cursors): How to send offset positions for a partition to Nakadi for storage.
  - [Checking Current Position](#checking-current-position): How to determine the current offsets for a Subscription.
  - [Subscription Statistics](#subscription-statistics): Viewing metrics for a Subscription.
  - [Deleting a Subscription](#deleting-a-subscription): How to remove a Subscription.
  - [Getting and Listing Subscriptions](#getting-and-listing-subscriptions): How to view individual an subscription and list existing susbcriptions.

For a more detailed description and advanced configuration options please take a look at Nakadi [swagger](api/nakadi-event-bus-api.yaml) file.

#### Creating Subscriptions

A Subscription can be created by posting to the `/subscriptions` collection resource:

```sh
curl -v -XPOST "http://localhost:8080/subscriptions" -H "Content-type: application/json" -d '{
    "owning_application": "order-service",
    "event_types": ["order.ORDER_RECEIVED"]
  }'    
```

The response returns the whole Subscription object that was created, including the server generated `id` field:

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

#### Consuming Events from a Subscription

Consuming events is done by sending a GET request to the Subscriptions's event resource (`/subscriptions/{subscription-id}/events`): 

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/events"
```

The response is a stream that groups events into JSON batches separated by an endline (`\n`) character. The output looks like this:

```sh
HTTP/1.1 200 OK
X-Nakadi-StreamId: 70779f46-950d-4e48-9fca-10c413845e7f
Transfer-Encoding: chunked

{"cursor":{"partition":"5","offset":"543","event_type":"order.ORDER_RECEIVED","cursor_token":"b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"},"info":{"debug":"Stream started"}]}
{"cursor":{"partition":"5","offset":"544","event_type":"order.ORDER_RECEIVED","cursor_token":"a28568a9-1ca0-4d9f-b519-dd6dd4b7a610"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"}]}
{"cursor":{"partition":"5","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"a241c147-c186-49ad-a96e-f1e8566de738"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"}]}
{"cursor":{"partition":"0","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"bf6ee7a9-0fe5-4946-b6d6-30895baf0599"}}
{"cursor":{"partition":"1","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"9ed8058a-95be-4611-a33d-f862d6dc4af5"}}
```

Each batch contains the following fields:

- `cursor`: The cursor of the batch which should be used for committing the batch.

- `events`: The array of events of this batch.

- `info`: An optional field that can hold useful information (e.g. the reason why the stream was closed by Nakadi).

Please also note that when stream is started, the client receives a header `X-Nakadi-StreamId` which must be used when committing cursors.

To see a full list of parameters that can be used to control a stream of events, please see 
an API specification in [swagger](api/nakadi-event-bus-api.yaml) file.

#### Client Rebalancing

If you need more than one client for your subscription to distribute load or increase throughput - you can read the subscription with multiple clients and Nakadi will automatically balance the load across them.

The balancing unit is the partition, so the number of clients of your subscription can't be higher 
than the total number of all partitions of the event-types of your subscription. 

For example, suppose you had a subscription for two event-types `A` and `B`, with 2 and 4 partitions respectively. If you start reading events with a single client, then the client will get events from all 6 partitions. If a second client connects, then 3 partitions will be transferred from first client to a second client, resulting in each client consuming 3 partitions. In this case, the maximum possible number of clients for the subscription is 6, where each client will be allocated 1 partition to consume.

The Subscription API provides a guarantee of _at-least-once_ delivery. In practice this means clients can see a duplicate event in the case where there are errors [committing events](#committing-cursors).  However the events which were successfully committed will not be resent. 

A useful technique to detect and handle duplicate events on consumer side is to be idempotent and to check `eid` field of event metadata. Note: `eid` checking is not possible using the "undefined" category, as it's only supplied in the "business" and "data" categories.


#### Subscription Cursors

The cursors in the Subscription API have the following structure:

```json
{
  "partition": "5",
  "offset": "543",
  "event_type": "order.ORDER_RECEIVED",
  "cursor_token": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
}
```

The fields are:

- `partition`: The partition this batch belongs to. A batch can only have one partition.

- `offset`: The offset of this batch. The offset is server defined and opaque to the client - clients should not try to infer or assume a structure. 

- `event_type`: Specifies the event-type of the cursor (as in one stream there can be events of different event-types);

- `cursor_token`: The cursor token generated by Nakadi.

#### Committing Cursors

Cursors can be committed by posting to Subscription's cursor resource (`/subscriptions/{subscriptionId}/cursors`), for example:

```sh
curl -v -XPOST "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/cursors"\
  -H "X-Nakadi-StreamId: ae1e39c3-219d-49a9-b444-777b4b03e84c" \
  -H "Content-type: application/json" \
  -d '{
    "items": [
      {
        "partition": "0",
        "offset": "543",
        "event_type": "order.ORDER_RECEIVED",
        "cursor_token": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
      },
      {
        "partition": "1",
        "offset": "923",
        "event_type": "order.ORDER_RECEIVED",
        "cursor_token": "a28568a9-1ca0-4d9f-b519-dd6dd4b7a610"
      }
    ]
  }'
```

Please be aware that `X-Nakadi-StreamId` header is required when doing a commit. The value should be the same as you get in `X-Nakadi-StreamId` header when opening a stream of events. Also, each client can commit only the batches that were sent to it.

The possible successful responses for a commit are:

- `204`: cursors were successfully committed and offset was increased.

- `200`: cursors were committed but at least one of the cursors didn't increase the offset as it was less or equal to already committed one. In a case of this response code user will get a json in a response body with a list of cursors and the results of their commits.

The timeout for commit is 60 seconds. If you open the stream, read data and don't commit
anything for 60 seconds - the stream connection will be closed from Nakadi side. Please note
that if there are no events available to send and you get only empty batches - there is no need
to commit, Nakadi will close connection only if there is some uncommitted data and no
commits happened for 60 seconds.

If the connection is closed for some reason then the client still has 60 seconds to commit the events it received from the moment when the events were sent. After that the session
will be considered closed and it will be not possible to do commits with that `X-Nakadi-StreamId`.
If the commit was not done - then the next time you start reading from a subscription you
will get data from the last point of your commit, and you will again receive the events you
haven't committed.

When a rebalance happens and a partition is transferred to another client - the commit timeout
of 60 seconds saves the day again. The first client will have 60 seconds to do the commit for that partition, after that the partition is started to stream to a new client. So if the commit wasn't done in 60 seconds then the streaming will start from a point of last successful commit. In other case if the commit was done by the first client - the data from this partition will be immediately streamed to second client (because there is no uncommitted data left and there is no need to wait any more).

It is not necessary to commit each batch. When the cursor is committed, all events that
are before this cursor in the partition will also be considered committed. For example suppose the offset was at `e0` in the stream below,

```
partition: [ e0 | e1 | e2 | e3 | e4 | e5 | e6 | e7 | e8 | e9 ]
     offset--^
```

and the stream sent back three batches to the client, where the client committed batch 3 but not batch 1 or batch 2,

```
partition: [ e0 | e1 | e2 | e3 | e4 | e5 | e6 | e7 | e8 | e9 ]
     offset--^       
                |--- batch1 ---|--- batch2 ---|--- batch3 ---|
                        |             |               |
                        v             |               | 
                [ e1 | e2 | e3 ]      |               |
                                      v               |
                               [ e4 | e5 | e6 ]       |
                                                      v
                                              [ e7 | e8 | e9 ]
                                                    
client: cursor commit --> |--- batch3 ---|
```

then the offset will be moved all the way up to `e9` implicitly committing all the events that were in the previous batches 1 and 2,

```
partition: [ e0 | e1 | e2 | e3 | e4 | e5 | e6 | e7 | e8 | e9 ]
                                                          ^-- offset
```

 
#### Checking Current Position

You can also check the current position of your subscription:

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/cursors"
```

The response will be a list of current cursors that reflect the last committed offsets:

```
HTTP/1.1 200 OK
{
  "items": [
    {
      "partition": "0",
      "offset": "8361",
      "event_type": "order.ORDER_RECEIVED",
      "cursor_token": "35e7480a-ecd3-488a-8973-3aecd3b678ad"
    },
    {
      "partition": "1",
      "offset": "6214",
      "event_type": "order.ORDER_RECEIVED",
      "cursor_token": "d1e5d85e-1d8d-4a22-815d-1be1c8c65c84"
    }
  ]
}
```

#### Subscription Statistics

The API also provides statistics on your subscription:

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/stats"
```

The output will contain the statistics for all partitions of the stream:

```
HTTP/1.1 200 OK
{
  "items": [
    {
      "event_type": "order.ORDER_RECEIVED",
      "partitions": [
        {
          "partition": "0",
          "state": "reassigning",
          "unconsumed_events": 2115,
          "stream_id": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
        },
        {
          "partition": "1",
          "state": "assigned",
          "unconsumed_events": 1029,
          "stream_id": "ae1e39c3-219d-49a9-b444-777b4b03e84c"
        }
      ]
    }
  ]
}
```

#### Deleting a Subscription

To delete a Subscription, send a DELETE request to the Subscription resource using its `id` field (`/subscriptions/{id}`):

```sh
curl -v -X DELETE "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f"
```

Successful response:

```
HTTP/1.1 204 No Content
```

#### Getting and Listing Subscriptions

To view a Subscription send a GET request to the Subscription resource resource using its `id` field (`/subscriptions/{id}`): :

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f"
```

Successful response:

```
HTTP/1.1 200 OK
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

To get a list of subscriptions send a GET request to the Subscription collection resource:

```sh
curl -v -XGET "http://localhost:8080/subscriptions"
```

Example answer:

```
HTTP/1.1 200 OK
{
  "items": [
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
  ],
  "_links": {
    "next": {
      "href": "/subscriptions?offset=20&limit=20"
    }
  }
}
```

It's possible to filter the list with the following parameters: `event_type`, `owning_application`.  
Also, the following pagination parameters are available: `offset`, `limit`.


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
* [x] creation of event types
* [x] low-level interface
    * manual client side partition management is needed
    * no support of commits
* [x] high-level interface (Subscription API)
    * automatic redistribution of partitions between consuming clients
    * commits should be issued to move server-side cursors
* [ ] Support of event filtering per subscriptions

## Contributing

Nakadi accepts contributions from the open-source community. Please see the [issue tracker](https://github.com/zalando/nakadi/issues) for things to work on.

Before making a contribution, please let us know by posting a comment to the relevant issue. And if you would like to propose a new feature, do start a new issue explaining the feature you’d like to contribute.
