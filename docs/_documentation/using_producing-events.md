---
title: Producing Events
position: 7
---

## Producing Events

### Posting an Event

One or more events can be published by posting to an event type's stream.

The URI for a stream is a nested resource and based on the event type's name - for example the "widgets" event type will have a relative resource path called `/event-types/widgets/events`.

This example posts two events to the `order_received` stream:

```sh
curl -v -XPOST -H "Content-Type: application/json" https://localhost:8080/event-types/order_received/events -d '[
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

As shown above, the event stream accepts an array of events.

### Validation Strategies and Effective Schema

Each event sent to the stream will be validated relative to the [effective schema](#effective-schema) for the event type's category.

The validation behavior and the effective schema varies based on the event type's category. For example, because the example above is a 'business' category event type, as well as the fields defined in the event type's original schema, the events must also contain a `metadata` object with an `eid` and `occurred_at` fields in order to conform to the standard structure for that category.

Once the event is validated, it is placed into a partition and made available
to consumers. If the event is invalid, it is rejected by Nakadi.

### Enrichment Strategies

_@@@TODO_

### Event Ordering

The order of events in the posted array will be the order they are
published onto the event stream and seen by consumers. They are not re-ordered
based on any values or properties of the data.

Applications that need to order events for a particular entity or based on a identifiable key in the data should configure their event type with the `hash` partitioning strategy and name the fields that can be used to construct the key. This allows partial ordering for a given entity.

Total ordering is not generally achievable with Nakadi (or Kafka) unless the partition size is configured to be size 1. In most cases, total ordering is not needed and in many cases is not desirable as it can severely limit system scalability and result in cluster hot spotting.

### Delivery versus Arrival Order

Nakadi preserves the order of events sent to it (the "arrival order"), but has
no control over the network between it and the producer. In some cases it
may be possible for events to leave the producer but arrive at Nakadi in a
different order (the "delivery order").

Not all events need ordering guarantees but producers that do need end to end
ordering have a few options they can take:

- Wait for a response from the Nakadi server before posting the next event. This trades off overall producer throughput for ordering.

- Use the `parent_eids` field in the 'business' and 'data' categories. This acts as a causality mechanism  by allowing events to have "parent" events. Note the `parent_eids` option is not available in the 'undefined' category.

- Define and document the ordering semantics as part of the event type's scheme definition such that a consumer could use the information to sequence events at their end.
