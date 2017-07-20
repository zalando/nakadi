---
title: Comparison
position: 11
---

## Comparison to Other Systems

In this section, we'll look at how Nakadi fits in with the stream broker/processing ecosystems. Notably we'll compare it to Apache Kafka, as that's a common question, but also look briefly at some of the main cloud offerings in this area.

  - [Apache Kafka](#kafka)
  - [Google Pub/Sub](#pubsub)
  - [AWS Kinesis](#kinesis)
  - [AWS Simple Queue Service (SQS)](#sqs)
  - [Allegro Hermes](#hermes)
  - [Azure EventHub](#eventhub)
  - [Confluent Platform](#confluent)

<a name="kafka"></a>
### Apache Kafka (version 0.9)

Relative to Apache Kafka, Nakadi provides a number of benefits while still leveraging the raw power of Kafka as its internal broker. 

- Nakadi has some characteristics in common with Kafka, which is to be expected as the Kafka community has done an excellent job in defining the space. The logical model is basically the same - streams have partitions, messages in a partition maintain their order, and there's no order across partitions. One producer can send an event to be read by multiple consumers and consumers have access to offset data that they can checkpoint. There are also some differences. For example Nakadi doens't expose Topics as a concept in its API. Instead there are Event Types that define structure and ownership details as well as the stream. Also consumers receive messages in batches and each batch is checkpointed rather than an individual message.

- Nakadi uses HTTP for communications. This lets microservices to maintain their boundaries and avoids forcing a shared technology dependency on producers and consumers - if you can speak HTTP you can use Nakadi and communicate with other services. This is a fairly subtle point, but Nakadi is optimised for general microservices integration and message passing, and not just handing off data to analytics subsystems. This means it needs to be available to as many different runtimes and stacks as possible, hence HTTP becomes the de-facto choice.

- Nakadi is designed to support autonomous service teams. In Zalando, where Nakadi originated, each team has autonomy and control of their microservices stack to let them move quickly and take ownership. When running on AWS, this extends all the way down - every team has their own account structure, and to ensure a level of security and compliance teams run standard AMIs and constrain how they interact to HTTPS using OAuth2 access controls. This means we tend to want to run any shared infrastructure as a service with a HTTP based interface. Granted, not everyone has this need - many shops on AWS won't have per-team account structures and will tend to use a smaller number of shared environments, but we've found it valulable to be able leverage the power of systems like Kafka in a way that fits in with this service architecture. 

- An event type registry with schema validation. Producers can define event types using JSON Schema. Having events validated against a published schema allows consumers to know they will.  There are projects in the Kafka ecosystem from Confluent that provide similar features such as the rest-proxy and the schema-registry, but they're slightly optimised for analytics, and not quite ideal for microservices where its more common to use regular JSON rather than Avro. The schema registry in particular is dependent on Avro. Also the consumer connection model for the rest-proxy requires clients are pinned to servers which complicates clients - the hope for the Nakadi is that its managed susbcription API, when that's available, will not require session affinity in this way.

- Inbuilt event types. Nakadi also optional support for events that describe business processes and data changes. These provide common primitives for event identity, timestamps, causality, operations on data and header propagation. Teams could define their own structures, but there's value in having some basic things that consumers and producers can coordinate on independent of the payload, and which are being checked before being propagated to multiple consumers.

- Operations is also a factor in Nakadi's design. Managing upgrades to systems like Kafka becomes easier when technology sits behind an API and isn't a shared dependency between microservices. Asychronous event delivery can be a simpler overall option for a microservice architecture compared to synchronized and deep call paths that have to be mitigated with caches, bulkheads and circuit breakers.

In short, Nakadi is best seen as a complement to Kafka. It allows teams to use Kafka within their own boundaries but not be forced into sharing it as a global dependency.

<a name="pubsub"></a>
### Google Pub/Sub

Like Nakadi, Pub/Sub has a HTTP API which hides details from producers and consumers and makes it suitable for use as a microservices backplane. There are some differences worth noting:

- Pub/Sub lets you acknowledge every message individually rather than checkpointing a position in a logical log. This approach makes its model fairly different to the other systems mentioned here. While it implies that there are no inbuilt ordering assurances it does allow consumers to be very precise about what they have received. 

- Pub/Sub requires a susbcription to be setup before messages can be consumed, which can then be used to manage delivery state for messages. In that sense it's not unlike a traditional queuing system where the server (or "broker") manages state for the consumer, with the slight twist that messages have a sort of random access for acknowledgements instead of competing for work at the top of queue. Nakadi may offer a similar subcription option in the future via a managed API, but today consumers are expected to manage their own offsets.

- Pub/Sub uses a polling model for consumers. Consumers grab a page of messages to process and acknowlege, and then make a new HTTP request to grab another page. Nakadi maintains a streaming connection to consumers, and will push events as they arrive.

- Pub/Sub uses a common envelope structure for producing and consuming messages, and does not define any higher level structures beyond that.

<a name="kinesis"></a>
### AWS Kinesis

Like Nakadi and Pub/Sub, AWS Kinesis has a HTTP API to hide its details. Kinesis and Nakadi are more similar to each other than Pub/Sub, but there are some differences.

- Kinesis expose shards (partitions) for a stream and supplies enough information to support per message checkpointing with semantics much like Kafka and Nakadi. Nakadi only supplies checkpointing information per batch of messages. Kinesis allows setting the partition hash key directly, whereas Nakadi computes the key based on the data. 

- Kinesis uses a polling model for consumers, whereas Nakadi maintains a streaming connection Kinesis consumers use a "shard iterator" to a grab pages of message, and then make a new HTTP request to grab another page. Kinesis limits the rate at which this can be done across all consumers (typically 5 transactions per second per open shard), which places an upper bound on consumer throughput. Kinesis has a broad range of choices for resuming from a position in the stream, Nakadi allows access only from the beginning and a named offset.

- Kinesis uses a common envelope structure for producing and consuming messages, and does not define any higher level structures beyond that. Payload data is submitted as an opaque base64 blob.

- AWS restrict the number of streams available to an account to quite a low starting number, and messages can be stored for a maximum of 7 days whereas Nakadi can support a large number of event types and the expiration for events is configurable.

- Kinesis supports resizing the number of shards in a stream wheres partition counts in Nakadi are fixed once set for an event type.


<a name="sqs"></a>
### AWS Simple Queue Service (SQS)

The basic abstraction in SQS is a queue, which is quite different from a Nakadi / Kafka stream.

- SQS queues are durable and highly available. A queue can hold an unlimited number of messages, with a maximum message retention of 2 weeks. Each message carries an opaque text payload (max. 256KB). In addition to that, messages can have up to 10 message attributes, which can be read without inspecting the payload.

- Each message in an SQS queue can only be consumed once. In the case of multiple consumers, each one would typically use a dedicated SQS queue, which are all hooked up to a shared Amazon SNS topic that provides the fanout. When a new consumer is later added to this setup, its queue will initially be empty. An SQS queue does not have any history, and cannot be "replayed" again like a Kafka stream.

- SQS has "work queue" semantics. This means that delivered messages have to be removed from the queue explicitly by a separate call. If this call is not received within a configured timeframe, the message is delivered again ("automatic retry"). After a configurable number of unsuccessful deliveries, the message is moved to a dead letter queue.

- In contrast to moving a single cursor in the datastream (like in Nakadi, Kinesis or Kafka), SQS semantics of confirming individual messages, has advantages if a single message is unprocessable (i.e. format is not parseable). In SQS only the problamatic message is delayed. In a cursor semantic the client has to decide: Either stop all further message processing until the problem is fixed or skip the message and move the cursor.

<a name="hermes"></a>
### Allegro Hermes

[Hermes](https://github.com/allegro/hermes) like Nakadi, is an API based broker build on Apache Kafka. There are some differences worth noting:

- Hermes uses webhooks to deliver messages to consumers. Consumers register a subscription with a callback url and a subscription policy that defines behaviors such as retries and delivery rates. Nakadi maintains a streaming connection to consumers, and will push events as they arrive. Whether messages are delivered in order to consumers does not appear to be a defined behaviour in the API. Similar to Kafka, Nakadi will deliver messages to consumers in arrival order for each partition. Hermes does not appear to support partitioning in its API. Hermes has good support for tracking delivered and undelivered messages to susbcribers.

- Hermes supports JSON Schema and Avro validation in its schema registry. Nakadi's registry currently only supports JSON Schema, but may support Avro in the future. Hermes does not provide inbuilt event types, whereas Nakadi defines optional types to support data change and business process events, with some uniform fields producers and consumers can coordinate on. 

- Hermes allows topics (event types in Nakadi) to be collated into groups that are adminstrated by a single publisher. Consumers access data at a per topic level, the same as Nakadi currently; Nakadi may support multi-topic subscriptions in the future via a subscription API. 

- The Hermes project supports a Java client driver for publishing messages. Nakadi does not ship with a client.

- Hermes claims resilience when it comes to issues with its internal Kafka broker, such that it will continue to accept messages when Kafka is down. It does this by buffering messages in memory with an optional means to spill to local disk; this will help with crashing brokers or hermes nodes, but not with loss of an instance (eg an ec2 instance). Nakadi does not accept messages if its Kafka brokers are down or unavailable. 

<a name="eventhub"></a>
### Azure Event Hub

_@@@ todo_

<a name="confluent"></a>
### Confluent Platform

_@@@ todo_
