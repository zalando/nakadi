---
title: Getting Started
position: 2
---

# Getting Started

In this section we'll walk through running a Nakadi service on your machine.
Once you have the service up and running, you can jump to
[Using Nakadi](#using_producing-events) to see how produce and consume messages.

## Quickstart

You can run a Nakadi service locally using Docker. If you don't have Docker
installed, there are great instructions available on
[the Docker website](https://www.docker.com/).

### Running a Server

From the project's home directory you can install and start a Nakadi container
via the `gradlew` command:

```sh
./gradlew startNakadi
```

This will start a docker container for the Nakadi server and another container
with its PostgreSQL, Kafka and Zookeeper dependencies. You can read more about
the `gradlew` script in the [Building and Developing section](#developing)

### Stopping a Server

To stop the running Nakadi:

```sh
./gradlew stopNakadi
```

---

### Notes

If you're having trouble getting started, you might find an answer in the
[Frequently Asked Questions (FAQ)](#faq) section of the documentation.

#### Ports

Some ports need to be available to run the service:

-  8080 for the API server
-  5432 for PostgreSQL
-  9092 for Kafka
-  2181 for Zookeeper

They allow the services to communicate with each other and should not be used
by other applications.

#### Mac OS and Docker

Since Docker for Mac OS runs inside Virtual Box, you will  want to expose
some ports first to allow Nakadi to access its dependencies -

```sh
docker-machine ssh default \
-L 9092:localhost:9092 \
-L 8080:localhost:8080 \
-L 5432:localhost:5432 \
-L 2181:localhost:2181
```

Alternatively you can set up port forwarding on the "default" machine through
its network settings in the VirtualBox UI, which look like this -

![vbox](./img/vbox.png)
