---
title: Authorization
position: 6
---

## Per-resource authorization

Nakadi allows users to restrict access to resources they own - currently, event types are the only resources supported, 
but we plan to extend this feature to subscriptions in the near future.

The authorization model is simple: policies can be attached to resources. A policy `P` defines which `subjects` can 
perform which `operations` on a resource `R`. To do so, the policy contains, for each operation, a list of attributes that 
represent subjects who are authorized to perform the operation on the resource. For a subject to be authorized, it needs 
to match at least one of these attributes (not necessarily all of them).

There are three kinds of operation: `admin`, to update the resource and delete it; `read`, to read events from the 
resource; and `write`, to write events to the resource.

An authorization request is represented by the tuple

`R(subject, operation, resource)`

The request will be approved iff the `resource` policy has at least one attribute for `operation` that matches the 
subject.

## Protecting an event type

Protecting an event type can be done either during the creation of the event type, or later, as an update to the event 
type. Users simply need to add an authorization section to their event type description, which looks like this:

```json
  "authorization": {
    "admins": [{"data_type": "user", "value": "bfawlty"}],
    "readers": [{"data_type": "user", "value": "bfawlty"}],
    "writers": [{"data_type": "user", "value": "bfawlty"}]
  }
```

In this section, the `admins` list includes the attributes that authorize a subject to perform the `admin` operation; 
the `readers` list includes the attributes that authorize a subject to perform the `read` operation; and the `writers` 
list includes the attributes that authorize a subject to perform the `write` operation;

Whenever an event type is created, or its authorization section is updated, all attributes are validated. The exact 
nature of the validation depends on the plugin implementation.

### Creating an event type

Here is a sample request with an authorization section. It gives read, write, and admin access to a single attribute, 
of type `service`:

```bash
curl -v -XPOST -H "Content-Type: application/json" https://localhost:8080/event-types -d '{
  "name": "order_received",
  "owning_application": "acme-order-service",
  "category": "business",
  "partition_strategy": "hash",
  "partition_key_fields": ["order_number"],
  "enrichment_strategies": ["metadata_enrichment"],
  "default_statistic": {
    "messages_per_minute": 1000,    
    "message_size":    5,
    "read_parallelism":    1,
    "write_parallelism": 1
  },
  "schema": {
    "type": "json_schema",
    "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }"
  },
  "authorization": {
    "admins": [{"data_type": "user", "value": "bfawlty"}],
    "readers": [{"data_type": "user", "value": "bfawlty"}],
    "writers": [{"data_type": "user", "value": "bfawlty"}]
  }
}'
```

### Updating an event type

Updating an event type is similar to creating one. Here is a sample request, that gives read, write, and admin access 
to the same application:

```bash
curl -v -XPUT -H "Content-Type: application/json" https://localhost:8080/event-types/order_received -d '{
  "name": "order_received",
  "owning_application": "acme-order-service",
  "category": "business",
  "partition_strategy": "hash",
  "partition_key_fields": ["order_number"],
  "enrichment_strategies": ["metadata_enrichment"],
  "default_statistic": {
    "messages_per_minute": 1000,    
    "message_size":    5,
    "read_parallelism":    1,
    "write_parallelism": 1
  },
  "schema": {
    "type": "json_schema",
    "schema": "{ \"properties\": { \"order_number\": { \"type\": \"string\" } } }"
  },
  "authorization": {
    "admins": [{"data_type": "user", "value": "bfawlty"}],
    "readers": [{"data_type": "user", "value": "bfawlty"}],
    "writers": [{"data_type": "user", "value": "bfawlty"}]
  }
}'
```

When updating an event type, users should keep in mind the following caveats:

- If the event type already has an authorization section, then it cannot be removed in an update;
- If the update changes the list of readers, then all consumers will be disconnected. It is expected that they will 
try to reconnect, which will only work for those that are still authorized. **WARNING**: this *also* applies to consumers 
using subscriptions; if a subscription includes multiple event types, and as a result of the update, a consumer loses 
read access to one of them, then the consumer will not be able to consume from the subscription anymore.
