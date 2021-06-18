---
title: Nakadi Concepts
position: 4
---

## Nakadi Concepts

The Nakadi API allows the publishing and consuming of _events_ over HTTP. 

A good way to think of events is that they are like messages in a stream processing or queuing system, but have a defined structure that can be understood and validated. The object containing the information describing an event is called an _event type_.

To publish and consume events, an _owning application_ must first register a new event type with Nakadi. The event type contains information such as its name, the aforementioned owning application, strategies for partitioning and enriching data, and a [JSON Schema](http://json-schema.org/). Nakadi supports an _event type registry_ API that lists all the available event types.

Once the event type is created, a resource called a _stream_ becomes available for that event type. The stream will accept events for the type from a _producer_ and can be read from by one or more _consumers_. Nakadi can validate each event that is sent to the stream.

An event type's stream can be divided into one or more _partitions_. Each event is placed into exactly one partition. Each partition represents an ordered log - once an event is added to a partition its position is never changed, but there is no global ordering across partitions [[1](#thelog)]. 

Consumers can read events and track their position in the stream using a _cursor_ that is given to each partition. Consumers can also use a cursor to read from a stream at a particular position. Multiple consumers can read from the same stream, allowing different applications to read the stream simultaneously. 

In summary, applications using Nakadi can be grouped as follows: 

- **Event Type Owners**: Event type owners interact with Nakadi via the event type registry to define event types based on a schema and create event streams. 

- **Event Producers**: Producers publish events to the event type's stream, that conform to the event type's schema.
 
- **Event Consumers**: Consumers read events from the event stream. Multiple consumers can read from the same stream.

----

<a class="anchor" href="#thelog" id="thelog"></a>
[1] For more detail on partitions and the design of streams see ["The Log"](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) by Jay Kreps.

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

The `offset` value of the cursor allows you to select where in the stream you 
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

