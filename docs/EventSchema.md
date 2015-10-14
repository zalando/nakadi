Event schema
============

`Event` is the core entity of the event processing system. The main goal of the standartization
of that format is to have a transparent way to exchange events between distributed applications.

DRAFT JSON-Schema definitions:
```yaml
definitions:
  Event:
    type: object
    required: [ event, partitioning_key, meta_data ]
    properties:
      event: { type: string }
      ordering_key: { type: string }
      meta_data:
        type: { $ref: '#/definitions/EventMetaData' }

  EventMetaData:
    type: object
    required: [ created, id, scopes ]
    properties:
      created: { type: string, format: data-time }
      id: { type: string, format: uuid }
      root_id: { type: string, format: uuid }
      parent_id: { type: string, format: uuid }
      scopes: { type: array, items: [ type: string ] }
```
