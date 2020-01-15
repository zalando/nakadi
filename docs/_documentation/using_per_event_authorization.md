---
title: Event-Based Authorization
position: 13
---

## Event-Based Authorization using EventAuthField

Nakadi provides an option to specify `event_auth_field` in an event type definition, which can be used to differentiate
 consumption of events in an event type on a per-consumer level. The `event_auth_field` takes the path and type of a
 field inside an event that can be then used by an authorization plugin to classify if a consumer should receive an
 event published to the event type.

 An `event_auth_field` field, hence, can be used to specify a classifier (which along with the authorization section)
 define who can read an event published to an event type.
 An `event_auth_field` in an event type definition can look, for instance like:

 ```
{
  "name": "order_received",
  "owning_application": "acme-order-service",
  ...
  "event_auth_field": {
    "type": "array",
    "path": "security.exclusive_readers"
  }
  "category": "business",
  ...
}
```

An event may be published to the above event type, and the logic for authorization then, could only allows a single
 reader/set of readers (out of many readers) to receive that event, for example:

```
  {
    "order_number": "24873243241",
    "security": {
      "exclusive_readers": ["team-A"]
    }
    "metadata": {
      "eid": "d765de34-09c0-4bbb-8b1e-7160a33a0791",
      "occurred_at": "2016-03-15T23:47:15+01:00"
    }
  }
```

 Also, once a `event_auth_field` is specified for an event type, it cannot be removed or updated.
