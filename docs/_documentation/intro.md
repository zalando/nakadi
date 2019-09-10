---
title: Introduction
position: 1
---

## Nakadi Event Broker

The goal of Nakadi (**ნაკადი** means "stream" in Georgian) is to provide an event broker infrastructure to:

- Abstract event delivery via a secured [RESTful API](https://zalando.github.io/nakadi/manual.html#nakadi-event-bus-api).
 
    This allows microservices teams to maintain service boundaries, and not directly depend on any specific message broker technology.
    Access can be managed individually for every Event Type and secured using *OAuth* and custom authorization plugins.

- Enable convenient development of event-driven applications and asynchronous microservices. 

    Event types can be defined with [Event type schemas](https://zalando.github.io/nakadi/manual.html#using_event-types) 
    and managed via a registry. All events will be validated against the schema before publishing the event type. 
    It allows to granite the data quality and data consistency for the data consumers.    
     
- Efficient low latency event delivery. 
    
    Once a publisher sends an event using a simple [HTTP POST](https://zalando.github.io/nakadi/manual.html#using_producing-events), 
    consumers can be pushed to via a [streaming](https://zalando.github.io/nakadi/manual.html#using_consuming-events-lola)
    HTTP connection, allowing near real-time event processing. 
    The consumer connection has keepalive controls and support for managing stream offsets using
    [subscriptions](https://zalando.github.io/nakadi/manual.html#using_consuming-events-hila). 

<img src="img/NakadiDeploymentDiagram.png" width="100%">

### Links

Read more to understand *The big picture* 
[Architecture for data integration](https://zalando.github.io/nakadi/manual.html#data_integration_at_zalando) 

Watch the talk [Data Integration in the World of Microservices](https://www.youtube.com/watch?v=SbVQBHRAFXA) 

### Development status

Nakadi is high-load production ready. 
Zalando uses Nakadi as its central Event Bus Service. 
Nakadi reliably handles the traffic from thousands event types with 
the throughput of more than hundreds gigabytes per second.
The project is in active development.
  
#### Features

* Stream:    
    * REST abstraction over Kafka-like queues.
    * CRUD for event types.
    * Event batch publishing.
    * Low-level interface.
        * manual client side partition management is needed
        * no support of commits
    * High-level interface (Subscription API).
        * automatic redistribution of partitions between consuming clients
        * commits should be issued to move server-side cursors
* Schema:    
    * Schema registry.
    * Several event type categories (Undefined, Business, Data Change).
    * Several partitioning strategies (Random, Hash, User defined).
    * Event enrichment strategies.
    * Schema evolution.
    * Events validation using an event type schema.
* Security:          
    * OAuth2 authentication.
    * Per-event type authorization.
    * Blacklist of users and applications.    
* Operations:    
    * [STUPS](https://stups.io/) platform compatible.    
    * [ZMON](https://zmon.io/) monitoring compatible.
    * SLO monitoring.
    * Timelines. 
        * This allows transparently switch production and consumption to different cluster (tier, region, AZ) without
        moving actual data and any service degradation.
        * Opens possibility for implementation of other streaming technologies and engines besides Kafka 
        (like AWS Kinesis, Google pub/sub etc.)
 

#### Additional features that we plan to cover in the future are:

* Support for different streaming technologies and engines. Nakadi currently uses [Apache Kafka](http://kafka.apache.org/) 
    as its broker, but other providers (such as Kinesis) will be possible.     
* Filtering of events for subscribing consumers.
* Store old published events forever using transparent fall back backup shortages like AWS S3. 
* Separate the internal schema register to standalone service.
* Use additional schema formats and protocols like Avro, protobuf and [others](https://en.wikipedia.org/wiki/Comparison_of_data_serialization_formats).

#### Related projects

The [zalando-nakadi](https://github.com/zalando-nakadi/) organisation contains many useful related projects
like

* Client libraries
* SDK
* GUI
* DevOps tools and more



## Examples

### Creating Event Types

An event type can be created by posting to the `event-types` resource.

```sh
curl -v -XPOST http://localhost:8080/event-types -H "Content-type: application/json" -d '{
  "name": "order.ORDER_RECEIVED",
  "owning_application": "order-service",
  "category": "undefined",
  "partition_strategy": "random",
  "schema": {
    "type": "json_schema",
    "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }"
  }
}'
```

### Publishing Events

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


### Consuming Events

You can open a stream for an Event Type via the `events` sub-resource:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/events 
    

HTTP/1.1 200 OK

{"cursor":{"partition":"0","offset":"4"},"events":[{"order_number": "ORDER_001", "metadata": {"eid": "4ae5011e-eb01-11e5-8b4a-1c6f65464fc6", "occurred_at": "2016-03-15T23:56:11+01:00"}}]}
{"cursor":{"partition":"0","offset":"5"},"events":[{"order_number": "ORDER_002", "metadata": {"eid": "4bea74a4-eb01-11e5-9efa-1c6f65464fc6", "occurred_at": "2016-03-15T23:57:15+01:00"}}]}
{"cursor":{"partition":"0","offset":"6"},"events":[{"order_number": "ORDER_003", "metadata": {"eid": "4cc6d2f0-eb01-11e5-b606-1c6f65464fc6", "occurred_at": "2016-03-15T23:58:15+01:00"}}]}
```

## Nakadi community

There is a large ecosystem of projects around Nakadi. Check they out on [zalando-nakadi](https://github.com/zalando-nakadi/)

