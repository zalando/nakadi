Event schema
============

`Event` is the core entity of the event processing system. The main goal of the standartization
of that format is to have a transparent way to exchange events between distributed applications.

DRAFT JSON-Schema definitions:
```yaml
definitions:
  Event:
    type: object
    description: "NOTE: Swagger definition currently not showing possibility of arbitrary fields"
    required:
      - event
      - partitioning_key
      - meta_data
    properties:
      event:
         type: string
         example: "https://resource-events.zalando.com/ResourceCreated"
      partitioning_key:
         type: string
         example: "ARTICLE:ABC123XXX-001"
      meta_data:
        $ref: '#/definitions/EventMetaData'

  EventMetaData:
    type: object
    required: [ id, created ]
    properties:
      id: { type: string, format: uuid }
      created: { type: string, format: data-time }
      root_id: { type: string, format: uuid }
      parent_id: { type: string, format: uuid }
      scopes:
        type: array
        items:
          type: string
```
