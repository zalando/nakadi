---
title: Event-Based Authorization
position: 13
---

## Event-Based Authorization using EventAuthField

Nakadi provides per-event filtering, allowing event type publishers to specify which consumers can read an event
 published to an event type. This can be achieved by defining the `event_auth_field` in an event type definition,
 pointing to a field in the published events, the value of which can then be used to implement per-event authorization.

 The `event_auth_field` takes the path to a string field, and type of the field inside an event, which can be then used
 by an authorization plugin to classify if a consumer should receive an event.
 If the field is absent or set to `null` in a published event, it is available for all consumers to read.

 An `event_auth_field` field can be used to specify a classifier (which along with the authorization section)
 defines who can read a published event.
 An `event_auth_field` in an event type definition can look, for instance like:

 ```
{
  "name": "order_received",
  "owning_application": "acme-order-service",
  ...
  "event_auth_field": {
    "type": "teams",
    "path": "security.exclusive_readers"
  }
  "category": "business",
  ...
}
```

An event may be published to the above event type, and the logic for authorization then, could only allow a single
 reader/set of readers (out of many readers) to receive that event, for example:

```
  {
    "order_number": "24873243241",
    "security": {
      "exclusive_readers": "Team-A"
    }
    "metadata": {
      "eid": "d765de34-09c0-4bbb-8b1e-7160a33a0791",
      "occurred_at": "2016-03-15T23:47:15+01:00"
    }
  }
```

 Also, once a `event_auth_field` is specified for an event type, it cannot be removed or updated.
