---
title: Getting Started
position: 2
---

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
[Frequently Asked Questions (FAQ)](#f-a-q) section of the documentation.

#### Ports

Some ports need to be available to run the service:

-  8080 for the API server
-  5432 for PostgreSQL
-  9092 and 29092 for Kafka
-  2181 for Zookeeper

Even though users of the API interact with port 8080, the other ports are exposed
in order to run integration tests.

If you are not running the tests, it's safe to modify docker-compose.yaml by removing
the port forwarding from dependencies.
