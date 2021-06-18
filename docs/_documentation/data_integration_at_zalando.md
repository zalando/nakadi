---
title: Data Integration at Zalando
position: 200
---

## Introduction

Teams running platform microservices encapsulate their datastores and data models in accordance with our [API Guidelines](http://zalando.github.io/restful-api-guidelines). The result is business intelligence and analysis, which are also teams running their own services, don’t have direct access to raw data. This demands a different approach to data integration compared to traditional techniques such as extract-transform-load (ETL) from application databases.

### Event Based Integration

The way we support data integration is twofold. First, teams take responsibility for providing data if there is demand for it or a platform requirement to do so, by publishing _Events_, defined according to an _Event Type_. Second, at the technical level, data integration happens by microservices publishing streams of event data to an _Event Broker_, which acts as managed infrastructure for the platform.

We can call this style _"Event Based Integration"_. It allows providing access to local microservice datastores as logical data synchronization events written into data lake and can be used to reconstruct state for BI and DS analysis and reporting purposes.

### Event Based Interchange

While we mostly focus on data _integation_ in this section, we should mention that the architectural style can also be used for _interchange_. This is where events are also used for business and other events used e.g. for asynchronous service to service communication or process monitoring purposes. These events also use event infrastructure and are also stored in data lake and provided for further analysis.

## Architectural Elements

In the platform architecture, there are two foundational services used for event based integration. First, an _Event Broker_, which is a managed deployment of the [Nakadi](https://nakadi.io) project. Second, the _Data Lake_, which captures and stores events from the broker, and is used by the data warehouse and other downstream systems.


![Event Based Integration - Architecture](./img/event_based_data_integration.svg)

### Data Model: Event Types and Events

Producers and consumers of events coordinate asynchronously via an event definition, called an _Event Type_, and its corresponding stream of _Events_ that conform to the type. Both event types and events are handled by the event broker, which supports a HTTP API.

Events types allow an _Event Producer_ to define a data contract based on a JSON-Schema and an _Event Category_. An event producer owns the definition of the events it publishes and is responsible for sending those events to the broker, registering the schema for the event type, and managing the versioning and evolution of the data. Events are part of the service's interface to the outside world.

_Event Consumers_ can connect to an HTTP event stream to receive events. Consumers can also use a _Subscription_ to have the broker manage their offset positions in an event stream. One event type can have many subscribing consumers, allowing new services to receive events without the producer having to know or directly integrate with each consumer.

### The Event Broker: Nakadi

Nakadi allows services to register types of event and have them published and enriched. Services can subscribe to events by type and consume them as a stream. Nakadi's publish/subscribe model preserves ordering for each received event, and supports expiration based retention and replay of events for its consumers.

Nakadi supports HTTP APIs for event publish-subscribe and schema management. This API constrains events on the platform to be one of three categories:

- Data Change Event: An event that represents a change to a record or other entity.

- Business Event: An event that is part of, or drives a business process.

- Undefined Event: Suitable for other kinds of events where schema validation still provides value.

Documentation on event types is available in the [Nakadi Developer Manual - Event Types](https://nakadi.io/manual.html#using_event-types).

### The Data Lake

A significant consumer of events is the *Data Lake*, which subscribes to multiple event types (specified by use case) sent to the broker. The data lake prepares and makes available platform data for use by the data warehouse and other analytical systems.

The Data Lake is not only capturing data for storage in the data lake, it also enables functions such as business process monitoring. For example, all EventLog events sent to the platform are captured by the Data Lake.
