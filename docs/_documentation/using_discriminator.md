---
title: Event-Based Authorization
position: 13
---

## Event-Based Authorization

Nakadi provides an option to specify per-event authorization. The event type definition accepts a `discriminator` object
 specifying the path and type of a field inside an event that describes the entities that can read that event. When
 consuming from an event type which specifies a discriminator, a consumer that can't be identified as an entity
 authorized to read it, will not receive the event. A discriminator in an event type definition can look,
 for instance like:

 ```
{
  "name": "order_received",
  "owning_application": "acme-order-service",
  ...
  "discriminator": {
    "type": "array",
    "path": "security.exclusive_readers"
  }
  "category": "business",
  ...
}
```

An event may be published to the above event type, which only allows a single reader (out of many readers) to receive
that event, for example:

```
  {
    "order_number": "24873243241",
    "security": {
      "exclusive_readers": ["consumer-1"]
    }
    "metadata": {
      "eid": "d765de34-09c0-4bbb-8b1e-7160a33a0791",
      "occurred_at": "2016-03-15T23:47:15+01:00"
    }
  }
```

 Also, once a discriminator is specified for an event type, it cannot be removed or updated.
