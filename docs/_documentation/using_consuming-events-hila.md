---
title: Subscriptions
position: 9
---
## Subscriptions

Subscriptions allow clients to consume events, where the Nakadi server store offsets and 
automatically manages reblancing of partitions across consumer clients. This allows clients 
to avoid managing stream state locally.

The typical workflow when using subscriptions is:

1. Create a Subscription specifying the event-types you want to read.

1. Start reading batches of events from the subscription. 

1. Commit the cursors found in the event batches back to Nakadi, which will store the offsets. 


If the connection is closed, and later restarted, clients will get events from 
the point of your last cursor commit. If you need more than one client for your 
subscription to distribute the load you can read the subscription with multiple 
clients and Nakadi will balance the load across them.

The following sections provide more detail on the Subscription API and basic 
examples of Subscription API creation and usage:

  - [Creating Subscriptions](#creating-subscriptions): How to create a new Subscription and select the event types.
  - [Consuming Events from a Subscription](#consuming-events-from-a-subscription): How to connect to and consume batches from a Susbcription stream.
  - [Client Rebalancing](#client-rebalancing): Describes how clients for a Subscription are automatically assigned partitions, and how the API's _at-least-once_ delivery guarantee works.
  - [Subscription Cursors](#subscription-cursors): Describes the structure of a Subscription batch cursor.
  - [Committing Cursors](#committing-cursors): How to send offset positions for a partition to Nakadi for storage.
  - [Checking Current Position](#checking-current-position): How to determine the current offsets for a Subscription.
  - [Subscription Statistics](#subscription-statistics): Viewing metrics for a Subscription.
  - [Deleting a Subscription](#deleting-a-subscription): How to remove a Subscription.
  - [Getting and Listing Subscriptions](#getting-and-listing-subscriptions): How to view individual an subscription and list existing susbcriptions.

For a more detailed description and advanced configuration options please take a look at Nakadi [swagger](https://github.com/zalando/nakadi/blob/master/docs/_data/nakadi-event-bus-api.yaml) file.

### Creating Subscriptions

A Subscription can be created by posting to the `/subscriptions` collection resource:

```sh
curl -v -XPOST "http://localhost:8080/subscriptions" -H "Content-type: application/json" -d '{
    "owning_application": "order-service",
    "event_types": ["order.ORDER_RECEIVED"]
  }'    
```

The response returns the whole Subscription object that was created, including the server generated `id` field:

```sh
HTTP/1.1 201 Created
Content-Type: application/json;charset=UTF-8

{
  "owning_application": "order-service",
  "event_types": [
    "order.ORDER_RECEIVED"
  ],
  "consumer_group": "default",
  "read_from": "end",
  "id": "038fc871-1d2c-4e2e-aa29-1579e8f2e71f",
  "created_at": "2016-09-23T16:35:13.273Z"
}
```

If there is already a subscription with same `owning_application`, `event_types` and `consumer_group`,
it is just returned (and not updated, all other parts of the request body are then ignored).

### Consuming Events from a Subscription

Consuming events is done by sending a GET request to the Subscriptions's event resource (`/subscriptions/{subscription-id}/events`): 

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/events"
```

The response is a stream that groups events into JSON batches separated by an endline (`\n`) character. The output looks like this:

```sh
HTTP/1.1 200 OK
X-Nakadi-StreamId: 70779f46-950d-4e48-9fca-10c413845e7f
Transfer-Encoding: chunked

{"cursor":{"partition":"5","offset":"543","event_type":"order.ORDER_RECEIVED","cursor_token":"b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"},"info":{"debug":"Stream started"}]}
{"cursor":{"partition":"5","offset":"544","event_type":"order.ORDER_RECEIVED","cursor_token":"a28568a9-1ca0-4d9f-b519-dd6dd4b7a610"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"}]}
{"cursor":{"partition":"5","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"a241c147-c186-49ad-a96e-f1e8566de738"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"}]}
{"cursor":{"partition":"0","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"bf6ee7a9-0fe5-4946-b6d6-30895baf0599"}}
{"cursor":{"partition":"1","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"9ed8058a-95be-4611-a33d-f862d6dc4af5"}}
```

Each batch contains the following fields:

- `cursor`: The cursor of the batch which should be used for committing the batch.

- `events`: The array of events of this batch.

- `info`: An optional field that can hold useful information (e.g. the reason why the stream was closed by Nakadi).

Please also note that when stream is started, the client receives a header `X-Nakadi-StreamId` which must be used when committing cursors.

To see a full list of parameters that can be used to control a stream of events, please see 
an API specification in [swagger](#nakadi-event-bus-api) file.

### Client Rebalancing

If you need more than one client for your subscription to distribute load or increase throughput - you can read the subscription with multiple clients and Nakadi will automatically balance the load across them.

The balancing unit is the partition, so the number of clients of your subscription can't be higher 
than the total number of all partitions of the event-types of your subscription. 

For example, suppose you had a subscription for two event-types `A` and `B`, with 2 and 4 partitions respectively. If you start reading events with a single client, then the client will get events from all 6 partitions. If a second client connects, then 3 partitions will be transferred from first client to a second client, resulting in each client consuming 3 partitions. In this case, the maximum possible number of clients for the subscription is 6, where each client will be allocated 1 partition to consume.

The Subscription API provides a guarantee of _at-least-once_ delivery. In practice this means clients can see a duplicate event in the case where there are errors [committing events](#committing-cursors).  However the events which were successfully committed will not be resent. 

A useful technique to detect and handle duplicate events on consumer side is to be idempotent and to check `eid` field of event metadata. Note: `eid` checking is not possible using the "undefined" category, as it's only supplied in the "business" and "data" categories.

### Subscription Cursors

The cursors in the Subscription API have the following structure:

```json
{
  "partition": "5",
  "offset": "543",
  "event_type": "order.ORDER_RECEIVED",
  "cursor_token": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
}
```

The fields are:

- `partition`: The partition this batch belongs to. A batch can only have one partition.

- `offset`: The offset of this batch. The offset is server defined and opaque to the client - clients should not try to infer or assume a structure. 

- `event_type`: Specifies the event-type of the cursor (as in one stream there can be events of different event-types);

- `cursor_token`: The cursor token generated by Nakadi.

### Committing Cursors

Cursors can be committed by posting to Subscription's cursor resource (`/subscriptions/{subscriptionId}/cursors`), for example:

```sh
curl -v -XPOST "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/cursors"\
  -H "X-Nakadi-StreamId: ae1e39c3-219d-49a9-b444-777b4b03e84c" \
  -H "Content-type: application/json" \
  -d '{
    "items": [
      {
        "partition": "0",
        "offset": "543",
        "event_type": "order.ORDER_RECEIVED",
        "cursor_token": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
      },
      {
        "partition": "1",
        "offset": "923",
        "event_type": "order.ORDER_RECEIVED",
        "cursor_token": "a28568a9-1ca0-4d9f-b519-dd6dd4b7a610"
      }
    ]
  }'
```

Please be aware that `X-Nakadi-StreamId` header is required when doing a commit. The value should be the same as you get in `X-Nakadi-StreamId` header when opening a stream of events. Also, each client can commit only the batches that were sent to it.

The possible successful responses for a commit are:

- `204`: cursors were successfully committed and offset was increased.

- `200`: cursors were committed but at least one of the cursors didn't increase the offset as it was less or equal to already committed one. In a case of this response code user will get a json in a response body with a list of cursors and the results of their commits.

The timeout for commit is 60 seconds. If you open the stream, read data and don't commit
anything for 60 seconds - the stream connection will be closed from Nakadi side. Please note
that if there are no events available to send and you get only empty batches - there is no need
to commit, Nakadi will close connection only if there is some uncommitted data and no
commits happened for 60 seconds.

If the connection is closed for some reason then the client still has 60 seconds to commit the events it received from the moment when the events were sent. After that the session
will be considered closed and it will be not possible to do commits with that `X-Nakadi-StreamId`.
If the commit was not done - then the next time you start reading from a subscription you
will get data from the last point of your commit, and you will again receive the events you
haven't committed.

When a rebalance happens and a partition is transferred to another client - the commit timeout
of 60 seconds saves the day again. The first client will have 60 seconds to do the commit for that partition, after that the partition is started to stream to a new client. So if the commit wasn't done in 60 seconds then the streaming will start from a point of last successful commit. In other case if the commit was done by the first client - the data from this partition will be immediately streamed to second client (because there is no uncommitted data left and there is no need to wait any more).

It is not necessary to commit each batch. When the cursor is committed, all events that
are before this cursor in the partition will also be considered committed. For example suppose the offset was at `e0` in the stream below,

```text
partition: [ e0 | e1 | e2 | e3 | e4 | e5 | e6 | e7 | e8 | e9 ]
     offset--^
```

and the stream sent back three batches to the client, where the client committed batch 3 but not batch 1 or batch 2,

```text
partition: [ e0 | e1 | e2 | e3 | e4 | e5 | e6 | e7 | e8 | e9 ]
     offset--^       
                |--- batch1 ---|--- batch2 ---|--- batch3 ---|
                        |             |               |
                        v             |               | 
                [ e1 | e2 | e3 ]      |               |
                                      v               |
                               [ e4 | e5 | e6 ]       |
                                                      v
                                              [ e7 | e8 | e9 ]
                                                    
client: cursor commit --> |--- batch3 ---|
```

then the offset will be moved all the way up to `e9` implicitly committing all the events that were in the previous batches 1 and 2,

```text
partition: [ e0 | e1 | e2 | e3 | e4 | e5 | e6 | e7 | e8 | e9 ]
                                                          ^-- offset
```

 
### Checking Current Position

You can also check the current position of your subscription:

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/cursors"
```

The response will be a list of current cursors that reflect the last committed offsets:

```json
HTTP/1.1 200 OK
{
  "items": [
    {
      "partition": "0",
      "offset": "8361",
      "event_type": "order.ORDER_RECEIVED",
      "cursor_token": "35e7480a-ecd3-488a-8973-3aecd3b678ad"
    },
    {
      "partition": "1",
      "offset": "6214",
      "event_type": "order.ORDER_RECEIVED",
      "cursor_token": "d1e5d85e-1d8d-4a22-815d-1be1c8c65c84"
    }
  ]
}
```

### Subscription Statistics

The API also provides statistics on your subscription:

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f/stats"
```

The output will contain the statistics for all partitions of the stream:

```json
HTTP/1.1 200 OK
{
  "items": [
    {
      "event_type": "order.ORDER_RECEIVED",
      "partitions": [
        {
          "partition": "0",
          "state": "reassigning",
          "unconsumed_events": 2115,
          "stream_id": "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
        },
        {
          "partition": "1",
          "state": "assigned",
          "unconsumed_events": 1029,
          "stream_id": "ae1e39c3-219d-49a9-b444-777b4b03e84c"
        }
      ]
    }
  ]
}
```

### Deleting a Subscription

To delete a Subscription, send a DELETE request to the Subscription resource using its `id` field (`/subscriptions/{id}`):

```sh
curl -v -X DELETE "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f"
```

Successful response:

```text
HTTP/1.1 204 No Content
```

### Getting and Listing Subscriptions

To view a Subscription send a GET request to the Subscription resource resource using its `id` field (`/subscriptions/{id}`): :

```sh
curl -v -XGET "http://localhost:8080/subscriptions/038fc871-1d2c-4e2e-aa29-1579e8f2e71f"
```

Successful response:

```
HTTP/1.1 200 OK
{
  "owning_application": "order-service",
  "event_types": [
    "order.ORDER_RECEIVED"
  ],
  "consumer_group": "default",
  "read_from": "end",
  "id": "038fc871-1d2c-4e2e-aa29-1579e8f2e71f",
  "created_at": "2016-09-23T16:35:13.273Z"
}
```

To get a list of subscriptions send a GET request to the Subscription collection resource:

```sh
curl -v -XGET "http://localhost:8080/subscriptions"
```

Example answer:

```json
HTTP/1.1 200 OK
{
  "items": [
    {
      "owning_application": "order-service",
      "event_types": [
        "order.ORDER_RECEIVED"
      ],
      "consumer_group": "default",
      "read_from": "end",
      "id": "038fc871-1d2c-4e2e-aa29-1579e8f2e71f",
      "created_at": "2016-09-23T16:35:13.273Z"
    }
  ],
  "_links": {
    "next": {
      "href": "/subscriptions?offset=20&limit=20"
    }
  }
}
```

It's possible to filter the list with the following parameters: `event_type`, `owning_application`.  
Also, the following pagination parameters are available: `offset`, `limit`.

