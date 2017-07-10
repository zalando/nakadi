# Resource Authorization

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

### Updating an event type

Updating an event type is similar to creating one. Here is a sample request, that gives read, write, and admin access 
to the same application:

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

When updating an event type, users should keep in mind the following caveats:

- If the event type already has an authorization section, then it cannot be removed in an update;
- If the update changes the list of readers, then all consumers will be disconnected. It is expected that they will 
try to reconnect, which will only work for those that are still authorized.

## Implementation

The implementation of per-resource authorization comprises of three components: a Policy Administration Point (PAP), 
where users can define and update the authorization policy for an event type; a Policy Decision Point (PDP), where 
a decision is made whether or not to authorize a subject to perform an operation in a resource (this component is 
implemented in a separate plugin); and a Policy Enforcement Point (PEP), which enforces the PDP's decisions.

### Default plugin

The default plugin is part of the Nakadi codebase. It authorizes all requests, and validates all attributes. If you 
want to deploy Nakadi and use the resource authorization feature, you should provide your own plugin.

### Building your own

You can provide your own PDP implementation using a plugin. To do so, you will need to implement two interfaces, which 
you can find in [nakadi-plugin-api](https://github.com/zalando-nakadi/nakadi-plugin-api): 
`org.zalando.nakadi.plugin.api.authz.AuthorizationService`, and 
`org.zalando.nakadi.plugin.api.authz.AuthorizationServiceFactory`. You can specify the plugin you want to 
use by setting the property `plugins.authz.factory` to use the factory you provided.

## FAQ

### Can I update an existing resource to add an authorization section?

Yes, you can.

### Who can update an existing resource to add an authorization section?

Anyone can, so please make sure to include the authorization section right from the start. For existing event types, 
try to add the authorization section yourself, as soon as possible.

### If I have admin rights, do I automatically get read and write rights?

No, not necessarily. But you can assign these rights yourself, since you are admin.

### What happens to connected consumers when an event type's readers section is modified?

All consumers will be disconnected. It is expected that they will try to reconnect; if they are still authorized to 
read from the event type, they will be able to do so.

### Does restricting readers in an event type affect the low level API, the high level (subscription) API, or both?

It affects both.

### What happens with subscriptions for multiple event types?

To read from such a subscription, the consumer needs to have 'read' rights to every event type in the subscription

### If my resource already has an authorization section, can I update the resource to remove the authorization section?

No, but you can add wildcards, if they are supported, so the resource will be 
open to all.

### How many attributes can I define in a single list?

There has to be at least one. There is no upper bound.

### Who else can access my resource, apart from those listed in the authorization section?

Administrators also have access to all resources.

### What happens if there is no authorization section in a resource?

In this case, all authenticated users can read from, write to, and administer the resource.