---
title: Event-Based Authorization
position: 13
---

## Event-Based Authorization

Nakadi provides per-event filtering, allowing event type publishers to specify which consumers can read an event
 published to an event type. This can be achieved by defining the `event_owner_selector` in an event type definition,
 that will specify how to extract ownership information.

 The `event_owner_selector` defines following values: 
  - `type` - the way how nakadi will extract owner from published events
  - `name` - the name of authorization_parameter that will be extracted and stored with event. 
  This name is used as `AuthorizationAttribute` data_type for security checks with authz plugin.
  - `value` - parameter that defines the way of extracting `AuthorizationAttribute` value 
  according to `type`.

 In case if `event_owner_selector` is set in event type, then resolution of authorization parameter
  value should succeed with non null value, otherwise publishing will be blocked.  
 
 ```
{
  "name": "order_received",
  "owning_application": "acme-order-service",
  ...
  "event_owner_selector": {
    "type": "path",
    "name": "retailer_id",
    "value": "security.exclusive_readers"
  }
  "category": "business",
  ...
}
```

 The events that were published to the event type above could be read only by a set of readers, that has matching 
 `retailer_id` provided by authorization plugin. In case if consumer do not have this value - the events are silently 
 omitted from the output.   

 Also, once a `event_owner_selector` is specified for an event type, it cannot be removed or updated.

 There are following event owner selector types supported: 
  - `path` - dot separated path within published event (after enrichment), in this case `value` 
  should hold dot separated path to a field that will be used as `AuthorizationParameter` value. 
  - `static` - all events, that are published to nakadi will have the same `AuthorizationParameter` 
  value, equal to `event_owner_selector` `value`   field.
  
 During consumption, the consumer is checked through authorization plugin whether or not it is 
 allowed to read Event resource with `AuthorizationParameter` data_type equal to `event_owner_selector` name
 and extracted `value`. 
  
 The access is checked for all the events being sent. If the access for some events is not allowed, 
 then the events are filtered out from the stream (not sent to consumer).
   
 Also, filtered out events are automatically committed when subscription API is used.  