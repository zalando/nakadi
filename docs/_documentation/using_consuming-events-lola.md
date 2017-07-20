---
title: Low-level API
position: 8
---

## Consuming Events with the Low-level API

### Connecting to a Stream

A consumer can open the stream for an Event Type via the `/events` sub-resource. For example to connect to the `order_received` stream send a GET request to its stream as follows:

```sh
curl -v https://localhost:8080/event-types/order_received/events 
```

The stream accepts various parameters from the consumer, which you can read about in the ["API Reference"](#nakadi-event-bus-api).
In this section we'll just describe the response format, along with how cursors and keepalives work.

### HTTP Event Stream

The HTTP response on the wire will look something like this (the newline is show as `\n` for clarity):

```sh
curl -v https://localhost:8080/event-types/order_received/events 
    

HTTP/1.1 200 OK
Content-Type: application/x-json-stream

{"cursor":{"partition":"0","offset":"6"},"events":[...]}\n
{"cursor":{"partition":"0","offset":"5"},"events":[...]}\n
{"cursor":{"partition":"0","offset":"4"},"events":[...]}\n
```

Nakadi groups events into batch responses (see the next section, "Batch Responses" for some more details). Batches are separated by a newline and each available batch will be emitted on a single line. If there are no new batches the server will occasionally emit an empty batch (see the section "Event Stream Keepalives" further down).

Technically, while each batch is a JSON document, the overall response is not valid JSON. For this reason it is served as the media type `application/x-stream-json` rather than `application/json`. Consumers can use the single line delimited structure to frame data for JSON parsing.

### Batch Response Formats

A pretty-printed batch object looks like this -

```json
{
  "cursor": {
    "partition": "0",
    "offset": "4"
  },
  "events": [...]
} 
```

Each batch belongs to a single partition. The `cursor` object describes the partition and the offset for this batch of events. The cursor allow clients to checkpoint their position in the stream's partition. Note that individual events in the stream don't have cursors, they live at the level of a batch. 

The `events` array contains a list of events that were published in the order they arrived from the producer. Note that while the producer can also send batches of events, there is no strict correlation between the batches the consumer is given and the ones the producer sends. Nakadi will regroup events send by the producer and distribute them across partitions as needed.

### Cursors and Offsets

By default the `/events` resource will return data from all partitions of an event type stream and will do so from the end (or "tail") of the stream. To select only particular partitions and a position in the stream to start, you can supply an `X-Nakadi-Cursors` header in the request:

```sh
curl -v https://localhost:8080/event-types/order_received/events \
  -H 'X-Nakadi-Cursors: [{"partition": "0", "offset":"12"}]'
```

The `X-Nakadi-Cursors` header value is a JSON array of _cursors_. Each cursor in the array describes its partition for the stream and an offset to stream from. 

The `offset` value of the cursor allows you select where in the stream partition you want to consume from. This can be any known offset value, or the dedicated value `begin` which will start the stream from the beginning. For example, to read from partition `0` from the beginning:

```sh
curl -v https://localhost:8080/event-types/order_received/events \
  -H 'X-Nakadi-Cursors:[{"partition": "0", "offset":"begin"}]'
```


### Event Stream Keepalives

If there are no events to be delivered the server will keep a streaming connection open by periodically sending a batch with no events but which contains a `cursor` pointing to the current offset. For example:

```sh
curl -v https://localhost:8080/event-types/order_received/events 
      

HTTP/1.1 200 OK
Content-Type: application/x-json-stream

{"cursor":{"partition":"0","offset":"6"},"events":[...]}\n
{"cursor":{"partition":"0","offset":"6"}}\n
{"cursor":{"partition":"0","offset":"6"}}\n
{"cursor":{"partition":"0","offset":"6"}}\n
```

This can be treated as a keep-alive control.
