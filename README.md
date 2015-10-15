Distributed Event Bus
=====================

> This is a prototype and a proof of concept project for now implemented in python.

The goal of the `nakadi` project (ნაკადი means `stream` in Georgian language) is to build a distributed event bus infrastrucutre that will enable different applications to:

* use common event format for data exchange;
* securly publish and consume events as easy as possible;
* register (or allow discovery of) the event types being published;

Why a distributed event bus?
----------------------------

The main drivers for the development of this project are several requirements, that we saw as important.

* event driven architecture
* team autonomy
* [standard API](/swagger.yaml) to abstract inter-team event exchange
* no organizational bottlenecks  
  (no additional bureaucratic bodies)
* no technological bottlenecks  
  (no need in one central service)

Some additional technical reqirements, that we wanted to cover by the an architecture 

* event ordering guarantees
* fast event processing
* scalable and highly available architecture
* secure
* [STUPS](https://stups.io/) compatible

To achive the goal of removing organizational and technological bottlenecks in the form of central services from one hand, 
and not to end up with a compleate chaos of not being able to understand dependencies between rapidly eveloving applications from the other, the project has envisioned the possibility for:

* discoverablitiy of the resource structures, flowing inside the distributed event bus
* discovery service, that will use these capabilities to collect resource schema information for an easy lookup by developers

What does the prototype already have?
=====================================

* [x] REST abstraction over Kafka-like queues
* [ ] supports event filtering per subscription
* streaming/batching of events to the clients
  * [x] low-level interface
    * manual client side partition distribution is needed
    * no support of commits
  * [ ] high-level interface
    * automatic redistribution of partitions between clients
    * commits should be issued to move server-side cursors
