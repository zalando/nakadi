Distributed Event Bus
=====================

> This is a prototype and a proof of concept project for now implemented in python.

The goal of the `nakadi` project (ნაკადი means `stream` in Georgian language) is to build a distributed event bus infrastrucutre that will enable different applications to:

* use [common event format](/docs/EventSchema.md) for data exchange
* securly publish and consume events as easy as possible
* allow standartized discovery of the event types being exchanged

Why a distributed event bus?
----------------------------

The main drivers for the development of this project are several requirements, that we saw as important.

* enable convient develpment of event driven applications
* team autonomy
* [standard API](/swagger.yaml) to abstract inter-team event exchange
* no organizational bottlenecks  
  (minimize the number of bureaucratic bodies)
* no technological bottlenecks  
  (no need in one central service)

Some additional technical reqirements, that we wanted to cover by the this architecture

* event ordering guarantees
* fast (real-time) event processing
* scalable and highly available architecture
* secure
* [STUPS](https://stups.io/) compatible

To achive the goal of removing organizational and technological bottlenecks in the form of central services from one hand, and not to end up with a compleate chaos of not being able to understand dependencies between rapidly eveloving applications from the other, the project has envisioned the possibility for:

* discoverablitiy of the resource structures, flowing inside this event bus
* centralized discovery service, that will use these capabilities to collect resource schema information for an easy lookup by developers

> NOTE: it is not really clear if the resource schema discoverability service should be part of Nakadi system

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
