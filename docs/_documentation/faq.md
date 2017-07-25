---
title: F.A.Q
position: 14
---

# Frequently Asked Questions

## Table of Contents

- [How long will events be persisted for?](#how-long-will-events-be-persisted-for)
- [How do I define how long will events be persisted for?](#how-do-i-define-how-long-will-events-be-persisted-for)
- [How many partitions will an event type be given?](#how-many-partitions-will-an-event-type-be-given)
- [How do I configure the number of partitions?](#how-do-i-configure-the-number-of-partitions)
- [Which partitioning strategy should I use?](#which-partitioning-strategy-should-i-use)
- [How can I keep track of a position in a stream?](#how-can-i-keep-track-of-a-position-in-a-stream)
- [What's an effective schema?](#whats-an-effective-schema)
- [Nakadi isn't validating metadata and/or event identifiers, what's going on?](#nakadi-isnt-validating-metadata-andor-event-identifiers-whats-going-on)
- [What clients are available?](#what-clients-are-available)
- [How do I disable OAuth for local development?](#how-do-i-disable-oauth-for-local-development)
- [I want to send arbitrary JSON, how do I avoid defining a JSON Schema?](#i-want-to-send-arbitrary-json-how-do-i-avoid-defining-a-json-schema)
- [Can I post something other than JSON as an event?](#can-i-post-something-other-than-json-as-an-event)
- [I get the message "Is the docker daemon running on this host?" - Help!](#i-get-the-message-is-the-docker-daemon-running-on-this-host---help)
- [What's the reason for newest available offset being bigger than oldest offset?](#whats-the-reason-for-newest-available-offset-being-bigger-than-oldest-offset)
- [Does Nakadi support compression?](#does-nakadi-support-compression)
- [How do I contribute to the project?](#how-do-i-contribute-to-the-project)

----


#### How long will events be persisted for?

The default retention time in the project is set by the `retentionMs` value in `application.yml`, which is currently 2 days. 

The service installation you're working with may have a different operational setting, and you should get in touch with the team operating that internal Nakadi service. 

#### How do I define how long will events be persisted for?

At the moment, retention can't be defined via the API per event type. It may be added as an option in the future. The best option for now would be to configure the underlying Kafka topic directly.

If you want to change the default for a server installation, you can set the `retentionMs` value in `application.yml` to a new value.

#### How many partitions will an event type be given?

The default partition count in the project is set by the `partitionNum` value in `application.yml`, which is currently `1`. 

The service installation you're working with may have a different operational setting, and you should get in touch with the team operating that internal Nakadi service. 

#### How do I configure the number of partitions?

At the moment, partition size can't be defined via the API per event type. It may be added as an option in the future. The best option for now would be to configure the underlying Kafka topic directly. 

If you want to change the default for a server installation, you can set the `partitionNum` value in `application.yml` to a new value.

#### Which partitioning strategy should I use?

See the section ["Partition Strategies"](#partition-strategies), which goes into more detail on the available options and what they're good for.

#### How can I keep track of a position in a stream?

Clients can track offset information sent in the Cursor on a per-partition basis - each batch of events sent to a consumer will contain such a Cursor that will detail the partition id and an offset (see ["Cursors and Offsets"](#cursors-and-offsets) for more information). This allows a client to track how far in the partition they have consumed events, and also allows them to submit a cursor with an appropriate value as described in the "Cursors and Offsets" section. One approach would be to use local storage (eg a datastore like PostgreSQL or DynamoDB) to record the position to date outside the client application, making it available in the event of restarts.

Note that a managed API is being developed which will supporting storing offsets for consumer clients in the future.

#### What's an effective schema?

The effective schema is the combination of the schema structure defined for a particular [category](#event-types-and-categories), such as 'business' or 'data' and the custom schema submitted when creating an event type. When an event is posted to Nakadi, the effective schema is used to validate the event and not the separate category level and custom level schemas. 

You can read more in the section ["Effective Schema"](#event-types). 

#### Nakadi isn't validating metadata and/or event identifiers, what's going on?

It's possible you are working with an 'undefined' event type. The 'undefined' category doesn't support metadata validation or enrichment. In more technical terms, the effective schema for an undefined event is exactly the same as the schema that was submitted when the event type was created.

#### What clients are available?

The project doesn't ship with a client, but there are a number of open source clients described in the ["Clients"](#using_clients) section.

If you have an open source client not listed there, we'd love to hear from you :) Let us know via GitHub and we'll add it to the list.

#### How do I disable OAuth for local development?

The default behavior when running the docker containers locally will be for OAuth to be disabled. 

If you are running a Nakadi server locally outside docker, you can disable token checks by setting the environment variable `NAKADI_OAUTH2_MODE` to `OFF` before starting the server.

Note that, even if OAuth is disabled using the `NAKADI_OAUTH2_MODE` environment variable, the current behavior will be to check a token if one is sent by a client so you might need to configure the client to also not send tokens.

#### I want to send arbitrary JSON, how do I avoid defining a JSON Schema?

The standard workaround is to define an event type with the following category and schema:

 - category: `undefined` 
 - schema: `{"additionalProperties": true}`

Note that sending a schema of `{}` means nothing will validate, not that anything will be allowed.

#### Can I post something other than JSON as an event?

It's a not a configuration the project directly supports or is designed for. But if you are willing to use JSON as a wrapper, one option is to define a JSON Schema with a property whose type is a string, and send the non-JSON content as a Base64 encoded value for the string. It's worth pointing out this is entirely opaque to Nakadi and you won't get the benefits of schema checking (or even that the submitted string is properly encoded Base64). Note that if you try this, you'll need to be careful to encode the Base64 as being URL/file safe to avoid issues with the line delimited stream format Nakadi uses to send messages to consumers - as mentioned this is an option that the server doesn't directly support.  

#### I get the message "Is the docker daemon running on this host?" - Help!

If you get the message "Is the docker daemon running on this host?" first check that Docker and VirtualBox are running. If you know they are running, you might want to run this command -

```sh
eval "$(docker-machine env default)"
```

#### What's the reason for newest available offset being bigger than oldest offset?

When there are no events available for an event-type because they've expired, then `newest_available_offset` will be smaller than `oldest_available_offset`. Because Nakadi has exclusive offset handling, it shows the offset of the last message in `newest_available_offset`.

#### Is there a way to make publishing batches of events atomic?

Not at the moment. If the events are for different event types, or the events will be distributed across different partitions for a single event type, then there's no way to achieve atomicity in the sense of "all events or no events will be published" in the general case. If the events belong to the same partition, the server does not have compensating behavior to ensure they will all be written.

Producers that need atomicity will want to create an event type structure that allows all the needed information to be contained within an event. This is a general distributed systems observation around message queuing and stream broker systems rather than anything specific to Nakadi.

#### Does Nakadi support compression?

The server will accept gzip encoded events when posted. On the consumer side, if the client asks for compression the server will honor the request.

#### How do I contribute to the project?

Nakadi accepts contributions from the open-source community. Please see the [project issue tracker](https://github.com/zalando/nakadi/issues) for things to work on. Before making a contribution, please let us know by posting a comment to the relevant issue. And if you would like to propose a new feature, do start a new issue explaining the feature you’d like to contribute.
