[![Build Status](https://travis-ci.org/zalando/nakadi.svg)](https://travis-ci.org/zalando/nakadi)

Distributed Event Bus
=====================

> This is a prototype and a proof of concept project for now implemented in python.

The goal of the `nakadi` project (ნაკადი means `stream` in Georgian language) is to build a distributed event bus infrastructure that will enable different applications to:

* use [common event format](/docs/EventSchema.md) for data exchange
* securely publish and consume events as easy as possible
* allow standardized discovery of the event types being exchanged

Why a distributed event bus?
----------------------------

The main drivers for the development of this project are several requirements that we saw as important:

* enable convenient development of event-driven applications
* team autonomy
* [standard API](/nakadi/swagger.yaml) to abstract inter-team event exchange
* no organizational bottlenecks  
  (minimize the number of bureaucratic bodies)
* no technological bottlenecks  
  (no need in one central service)

Some additional technical requirements that we wanted to cover by this architecture:

* event ordering guarantees
* fast (real-time) event processing
* scalable and highly available architecture
* security
* [STUPS](https://stups.io/) compatible

In order to achieve the goal of removing organizational and technological bottlenecks in the form of central services and additionally not to end up with a complete chaos of not being able to understand dependencies between rapidly evolving applications, the project has envisioned the possibility for:

* discoverability of the resource structures flowing into the event bus
* centralized discovery service, that will use these capabilities to collect resource schema information for easy lookup by developers

> NOTE: it is not really clear if the resource schema discoverability service should be part of `nakadi` system

What does the prototype already have?
=====================================

* [x] REST abstraction over Kafka-like queues
* [ ] support of event filtering per Subscription
* streaming/batching of events to/from the clients
  * [x] low-level interface
    * manual client side partition distribution is needed
    * no support of commits
  * [ ] high-level interface
    * automatic redistribution of partitions between consuming clients
    * commits should be issued to move server-side cursors
