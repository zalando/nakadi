---
title: Introduction
position: 1
---

# Nakadi

## Nakadi Event Broker

The goal of Nakadi (ნაკადი means "stream" in Georgian) is to provide an event broker infrastructure to:

#### RESTfull 

Abstract event delivery via a secured [RESTful API](#nakadi-event-bus-api). This allows microservices teams to maintain service boundaries, and not directly depend on any specific message broker technology. Access to the API can be managed and secured using OAuth scopes.

#### JSON Schema

Enable convenient development of event-driven applications and asynchronous microservices. Event types can be defined with schemas and managed via a registry. Nakadi also has optional support for events describing business processes and data changes using standard primitives for identity, timestamps, event types, and causality. 

#### Performance

Efficient low latency event delivery. Once a publisher sends an event using a simple HTTP POST, consumers can be pushed to via a streaming HTTP connection, allowing near real-time event processing. The consumer connection has keepalive controls and support for managing stream offsets. 

#### Scalability

Nakadi instances are stateless. They can be run on AWS with auto-scaling. 

#### Flexibility

Using Timelines it is easy to move the traffic to another cluster without moving the data and any service degradation. 


## Examples

### Creating Event Types

An event type can be created by posting to the `event-types` resource.

```sh
curl -v -XPOST https://localhost:8080/event-types -H "Content-type: application/json" -d '{
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
curl -v -XPOST https://localhost:8080/event-types/order.ORDER_RECEIVED/events -H "Content-type: application/json" -d '[
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
curl -v https://localhost:8080/event-types/order.ORDER_RECEIVED/events 
    

HTTP/1.1 200 OK

{"cursor":{"partition":"0","offset":"4"},"events":[{"order_number": "ORDER_001", "metadata": {"eid": "4ae5011e-eb01-11e5-8b4a-1c6f65464fc6", "occurred_at": "2016-03-15T23:56:11+01:00"}}]}
{"cursor":{"partition":"0","offset":"5"},"events":[{"order_number": "ORDER_002", "metadata": {"eid": "4bea74a4-eb01-11e5-9efa-1c6f65464fc6", "occurred_at": "2016-03-15T23:57:15+01:00"}}]}
{"cursor":{"partition":"0","offset":"6"},"events":[{"order_number": "ORDER_003", "metadata": {"eid": "4cc6d2f0-eb01-11e5-b606-1c6f65464fc6", "occurred_at": "2016-03-15T23:58:15+01:00"}}]}
```

## Nakadi community

Nakadi ecosystem has a lot of related projects. Please check [zalando-nakadi](https://github.com/zalando-nakadi/)

