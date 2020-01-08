---
title: Event-Based Authorization
position: 13
---

## Event-Based Authorization using Discriminator

Nakadi provides an option to specify discriminator in an event type definition, which can be used to differentiate
 consumption of events published on a per-consumer level. The `discriminator` takes the path and type of a field
 inside an event that can be used to implement the distinction using a Nakadi plugin.

 One use-case to specify the discriminator could be to implement per-event authorization. A discriminator field, hence
 can be used to specify identifiers of consumers that can read that event.
 A discriminator in an event type definition can look, for instance like:

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

An event may be published to the above event type, and the logic for authorization then, could only allows a single
 reader (out of many readers) to receive that event, for example:

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
