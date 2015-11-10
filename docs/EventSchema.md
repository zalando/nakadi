Event schema
============

`event` is the core entity of the event processing system. The main goal of the standartization
of that format is to have a transparent way to exchange events between distributed applications.

DRAFT JSON-Schema definitions:
```yaml
definitions:
  event:
    type: object
    description: |
      This is the most general representation of an event, that can be processed by Nakadi.
      It should be used as a base definition for all events, that flow through Nakadi by extending attributes of this object type.
    required:
      - event
      - partitioning_key
      - meta_data
    properties:
      event:
         type: string
         example: "https://resource-events.zalando.com/ResourceCreated"
         description: |
           Wo do not have a consensus about the name of the field yet. 
           `event` was chosen as a name here because it is used with name in
           [SSE](http://www.w3.org/TR/eventsource/) protocol.
           Another possibility would be to use more understandable `event_type`
      partitioning_key:
         type: string
         example: "ARTICLE:ABC123XXX-001"
         description: |
           A value by which the partition is to be  determined.
           Partitions are sequences of values within which the ordering is preserved
      meta_data:
        $ref: '#/definitions/event_meta_data'

  event_meta_data:
    type: object
    required: [ created ]
    properties:
      eid:
        description: globally unique event ID (assigned to the event by Nakadi system)
        type: string
        format: uuid
      created:
        description: Time (UTC) at which the event occurred in the source system (set by the client)
        type: string
        format: data-time
      root_eid:
        description: Event ID of the event that was the first in the chain of events, that lead to creation of the current event
        type: string
        format: uuid 
      parent_eid:
        description: Event ID of the event, that lead to producing of the current event
        type: string
        format: uuid
      scopes:
        description: Security scopes of the event (it is not clear if one needs to carry security scopes with every event)
        type: array
        items:
          type: string
```
