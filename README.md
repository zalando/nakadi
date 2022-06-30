## [Nakadi Event Broker](https://zalando.github.io/nakadi/)

[![Build Status](https://travis-ci.org/zalando/nakadi.svg?branch=master)](https://travis-ci.org/zalando/nakadi)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/785ccd4ab5e34867b760a8b07c3b62f1)](https://www.codacy.com/app/aruha/nakadi?utm_source=www.github.com&amp;utm_medium=referral&amp;utm_content=zalando/nakadi&amp;utm_campaign=Badge_Grade)

Nakadi is a distributed event bus broker that implements a RESTful API abstraction on top of Kafka-like queues, which can be used to send, receive, and analyze streaming data in real time, in a reliable and highly available manner.

One of the most prominent use cases of Nakadi is to decouple micro-services by building data streams between producers and consumers.

Main users of nakadi are developers and analysts. Nakadi provides features like REST based integration, multi consumer, ordered delivery, interactive UI, fully managed, security, ensuring data quality, abstraction of big data technology, and push model based consumption.

Nakadi is in active developement and is currently in production inside Zalando as the backbone of our microservices sending millions of events daily with a throughput of more than hundreds gigabytes per second. In one line, Nakadi is a **high-scalability data-stream for enterprise engineering teams**.  

![Nakadi Deployment Diagram](docs/img/NakadiDeploymentDiagram.png)

More detailed information can be found on our [website](http://zalando.github.io/nakadi/).

### Project goal

The goal of Nakadi (**ნაკადი** means *stream* in Georgian) is to provide an event broker infrastructure to:

- Abstract event delivery via a secured [RESTful API](https://zalando.github.io/nakadi/manual.html#nakadi-event-bus-api).

    This allows microservices teams to maintain service boundaries, and not directly depend on any specific message broker technology.
    Access can be managed individually for every queue and secured using *OAuth* and custom authorization plugins.

- Enable convenient development of event-driven applications and asynchronous microservices.

    Event types can be defined with [Event type schemas](https://zalando.github.io/nakadi/manual.html#using_event-types)
    and managed via a registry. All events will be validated against the schema before publishing.
    This guarantees data quality and consistency for consumers.    

- Efficient low latency event delivery.

    Once a publisher sends an event using a simple [HTTP POST](https://zalando.github.io/nakadi/manual.html#using_producing-events),
    consumers can be pushed to via a [streaming](https://zalando.github.io/nakadi/manual.html#using_consuming-events-lola)
    HTTP connection, allowing near real-time event processing.
    The consumer connection has keepalive controls and support for managing stream offsets using
    [subscriptions](https://zalando.github.io/nakadi/manual.html#using_consuming-events-hila).

### Development status

- Nakadi is high-load production ready.
- Zalando uses Nakadi as its central Event Bus Service.
- Nakadi reliably handles the traffic from thousands event types with the throughput of more than hundreds gigabytes per second.
- The project is in active development.  

### Presentations

- [Watch the talk on Nakadi at Fosdem](https://archive.fosdem.org/2018/schedule/event/nakadi/)
- [Background on the Zalando Microservices platform](https://www.youtube.com/watch?v=gEeHZwjwehs)

#### Features

* Stream:    
    * REST abstraction over Kafka-like queues.
    * CRUD for event types.
    * Event batch publishing.
    * Low-level interface (**deprecated**).
        * manual client side partition management is needed
        * no support of commits
    * High-level interface (Subscription API).
        * automatic redistribution of partitions between consuming clients
        * commits should be issued to move server-side cursors
* Schema:    
    * Schema registry.
    * Several event type categories (Undefined, Business, Data Change).
    * Several partitioning strategies (Random, Hash, User defined).
    * Event enrichment strategies.
    * Schema evolution.
    * Events validation using an event type schema.
* Security:          
    * OAuth2 authentication.
    * Per-event type authorization.
    * Blacklist of users and applications.    
* Operations:    
    * [STUPS](https://stups.io/) platform compatible.    
    * [ZMON](https://zmon.io/) monitoring compatible.
    * SLO monitoring.
    * Timelines:
        * this allows transparently switch production and consumption to different cluster (tier, region, AZ) without
        moving actual data and any service degradation.
        * opens the possibility for implementation of other streaming technologies and engines besides Kafka
        (like Amazon Kinesis or Google Cloud Pub/Sub)

Read more about latest development on the [releases page](https://github.com/zalando/nakadi/releases).

#### Additional features that we plan to cover in the future are:

* Support for different streaming technologies and engines. Nakadi currently uses [Apache Kafka](http://kafka.apache.org/)
    as its broker, but other providers (such as Kinesis) will be possible.     
* Filtering of events for subscribing consumers.
* Store old published events forever using transparent fall back backup shortages like AWS S3.
* Separate the internal schema register to standalone service.
* Use additional schema formats and protocols like Avro, protobuf and [others](https://en.wikipedia.org/wiki/Comparison_of_data_serialization_formats).

#### Related projects

The [zalando-nakadi](https://github.com/zalando-nakadi/) organisation contains many useful related projects
like

* [Use Nakadi with a client in your language of choice](https://nakadi.io/manual.html#using_clients),
* SDK,
* [Web UI](https://github.com/zalando-incubator/nakadi-ui),
* DevOps tools, and more.



## How to contribute to Nakadi

Read our [contribution guidelines](CONTRIBUTING.md) on how to submit issues and pull requests, then get Nakadi up and running locally using Docker:

### Dependencies

The Nakadi server is a Java 8 [Spring Boot](https://projects.spring.io/spring-boot/) application.
It uses [Kafka 1.1.1](https://kafka.apache.org/11/documentation.html) as its broker and
 [PostgreSQL 9.5](https://www.postgresql.org/docs/9.5/static/release-9-5.html) as its supporting database.

Nakadi requires recent versions of docker and docker-compose. In
particular, docker-compose >= v1.7.0 is required. See [Install Docker
Compose](https://docs.docker.com/compose/install/) for information on
installing the most recent docker-compose version.

The project is built with [Gradle](http://gradle.org).
The `./gradlew` [wrapper script](http://www.gradle.org/docs/current/userguide/gradle_wrapper.html) will bootstrap
the right Gradle version if it's not already installed.

### Install

To get the source, clone the git repository.
```sh
git clone https://github.com/zalando/nakadi.git
```
### Building

The gradle setup is fairly standard, the main tasks are:

- `./gradlew build`: run a build and test
- `./gradlew clean`: clean down the build

Some other useful tasks are:

- `./gradlew startNakadi`: build Nakadi and start docker-compose services: nakadi, postgresql, zookeeper and kafka
- `./gradlew stopNakadi`: shutdown docker-compose services
- `./gradlew startStorages`: start docker-compose services: postgres, zookeeper and kafka (useful for development purposes)
- `./gradlew fullAcceptanceTest`: start Nakadi configured for acceptance tests and run acceptance tests

For working with an IDE, the `eclipse` IDE task is available and you'll be able to import the `build.gradle` into Intellij IDEA directly.

### Running a Server

**Note**: Nakadi Docker for ARM processors is available at [here](./docker-arm)

From the project's home directory you can start Nakadi via Gradle:

```sh
./gradlew startNakadi
```

This will build the project and run docker compose with 4 services:

- Nakadi (8080)
- PostgreSQL (5432)
- Kafka (9092)
- Zookeeper (2181)

To stop the running Nakadi server:

```sh
./gradlew stopNakadi
```


## Using Nakadi and its API

Please read the [manual](https://zalando.github.io/nakadi/manual.html) for the full API usage details.

### Creating Event Types

The Nakadi API allows the publishing and consuming of _events_ over HTTP.
To do this the producer must register an _event type_ with the Nakadi schema
registry.

This example shows a minimalistic `undefined` category event type with a wildcard schema:

```sh
curl -v -XPOST http://localhost:8080/event-types -H "Content-type: application/json" -d '{
  "name": "order.ORDER_RECEIVED",
  "owning_application": "order-service",
  "category": "undefined",
  "schema": {
    "type": "json_schema",
    "schema": "{ \"additionalProperties\": true }"
  }
}'
```
**Note:** This is not a recommended category and schema. It should be used only for testing.

You can read more about this in the [manual](https://zalando.github.io/nakadi/manual.html#using_event-types).

### Consuming Events

You can open a stream for an event type via the `events` sub-resource:

```sh
curl -v http://localhost:8080/event-types/order.ORDER_RECEIVED/events


HTTP/1.1 200 OK

{"cursor":{"partition":"0","offset":"82376-000087231"},"events":[{"order_number": "ORDER_001"}]}
{"cursor":{"partition":"0","offset":"82376-000087232"}}
{"cursor":{"partition":"0","offset":"82376-000087232"},"events":[{"order_number": "ORDER_002"}]}
{"cursor":{"partition":"0","offset":"82376-000087233"},"events":[{"order_number": "ORDER_003"}]}
```
You will see the events when you publish them from another console for example.
The records without `events` field are `Keep Alive` messages.

**Note:** the [low-level API](https://zalando.github.io/nakadi/manual.html#using_consuming-events-lola) should be
used only for debugging. It is not recommended for production systems.
For production systems, please use the [Subscriptions API](https://zalando.github.io/nakadi/manual.html#using_consuming-events-hila).

### Publishing Events

Events for an event type can be published by posting to its "events" collection:

```sh
curl -v -XPOST http://localhost:8080/event-types/order.ORDER_RECEIVED/events \
 -H "Content-type: application/json" \
 -d '[{
    "order_number": "24873243241"
  }, {
    "order_number": "24873243242"
  }]'


HTTP/1.1 200 OK  
```

Read more in the [manual](https://zalando.github.io/nakadi/manual.html#using_producing-events).

## Contributing

Nakadi accepts contributions from the open-source community.

Please read [`CONTRIBUTING.md`](CONTRIBUTING.md).

Please also note our [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## Contact

This [email address](MAINTAINERS) serves as the main contact address for this project.

Bug reports and feature requests are more likely to be addressed
if posted as [issues](https://github.com/zalando/nakadi/issues) here on GitHub.

## License

Please read the full [`LICENSE`](LICENSE)

The MIT License (MIT) Copyright © 2015 Zalando SE, https://tech.zalando.com

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the “Software”), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
