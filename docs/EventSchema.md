Event schema
============

`Event` is the core entity of the event processing system. The main goal of the standartization
of that format is to have a transparent way to exchange events between distributed applications.

DRAFT JSON-Schema definitions:
```yaml
oneOf: 
  - $ref: "#/definitions/ResourceEvent"
  - $ref: "#/definitions/BusinessEvent"
definitions:
  EventMetaData:
    type: object
    properties:
      id: { type: string, format: uuid }
      event_time: { type: string, format: date-time, description: "time at which the event occured, UTC" }
      created: { type: string, format: date-time, description: "time at which this message was created, UTC" }
      root_id: { type: string, format: uuid }
      parent_id: { type: string, format: uuid }
      content_type: 
        type: string
        format: uri
        description: |
          the payload's content type, examples are:
          https://types.zalan.do/events/order-canceled
          https://types.zalan.do/resources/article-simple          
      partitioning_key: {type:string, description: "tbd."}
      scopes:
        type: array
        items: { type: string }
    required: [ id, event_time, content_type ]

  BusinessEvent:   
    properties:
      meta_data: { $ref:  "#/definitions/EventMetaData" }
      data:         { not: {} }
      data_key:     { not: {} }
      data_op_type: { not: {} }
    required: [ meta_data ]
    additionalProperties: {type: string, comment: " not nested structures allowed in business events? tbd." }

  ResourceEvent:
    properties:       
      meta_data: { $ref:  "#/definitions/EventMetaData" }
      data:
        description: "Complete state description of this very resource _after_ the event"
        type: object
      data_key: 
        type: object
        description: "1..n key/value pairs to uniquely identify this very resource amongst all resources of the given resource type"
      data_op_type:
        type: string
        description: "the modifying operation"
        enum: [ created, updated, deleted, audit ]
    required:
      - meta_data
      - data
      - data_key
      - data_op_type
```
