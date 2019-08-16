---
title: Authorization
position: 6
---

## Per-resource authorization

Nakadi allows users to restrict access to resources they own - both for event types and subscriptions.

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

## Protecting an event type or a subscription

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

Similarly, protecting a subscription can be done during it's creation or as an update to the subscription.
Authorization section for a subscription looks like below, and contains only the `admins` and the `readers` lists. The
list of admins specifies the attributes that can do admin operations like deletion or updating a subscription and i
the list of readers specifies the attributes that can read and commit to that subscription.

```json
  "authorization": {
    "admins": [{"data_type": "user", "value": "bfawlty"}],
    "readers": [{"data_type": "user", "value": "bfawlty"}],
  }
```

Whenever an event type and subscription is created, or its authorization section is updated, all attributes are validated. The exact 
nature of the validation depends on the plugin implementation.

### Creating an event type or a subscription with authorization

Here is a sample request with an authorization section. It gives read, write, and admin access to a single attribute, 
of type `service`:

```bash
curl -v -XPOST -H "Content-Type: application/json" http://localhost:8080/event-types -d '{
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

For creating a subscription, the sample request is as follows:

```bash
curl -v -XPOST -H "Content-Type: application/json" http://localhost:8080/subscriptions/ea2d7472-ddc6-4b9e-91f1-5bcd0b7b4fa4 -d '{
  "owning_application": "acme-order-service",
  "consumer_group": "acme-orders",
  "event-types": ["orders_received"],
  "authorization": {
    "admins": [{"data_type": "user", "value": "bfawlty"}],
    "readers": [{"data_type": "user", "value": "bfawlty"}],
  }
}'
```

### Updating an event type

Updating an event type or a subscription is similar to creating one. Here is a sample request, that gives read, write, 
and admin access to the same application (for an event-type):

```bash
curl -v -XPUT -H "Content-Type: application/json" http://localhost:8080/event-types/order_received -d '{
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

When updating an event type or subscription, users should keep in mind the following caveats:

- If the event type or subscription already has an authorization section, then it cannot be removed in an update;
- If the update changes the list of readers (for event-types and subscriptions), then all consumers will be disconnected.
It is expected that they will try to reconnect, which will only work for those that are still authorized.
 
**WARNING**: this *also* applies to consumers using subscriptions; if a subscription includes multiple event types, and as a result of the update, a consumer loses 
read access to one of them, then the consumer will not be able to consume from the subscription anymore.
