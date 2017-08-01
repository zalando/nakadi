---
title: Timelines
position: 101
---

## Timelines


This document covers Timelines internals. It's meant to explain how
timelines work, to help you understand the code and what each part of
it contributes to the overall picture.

### Fake timeline: your first timeline

Before timelines, Nakadi would connect to a single Kafka cluster,
which used to be specified in the application yaml file. This was a
static configuration loaded during boot time. Once timelines were
introduced, in order to ease the migration process and remove special
cases from the implementation, a fake timeline was introduced.

Fake timelines ease the transition from existent event types. The
migration process is as follow:

0. Initial state: all event types are associated to a Fake timeline.
1. Create first real timeline: this step creates an entry in the
   timeline table. But this timeline is still bound to the existent
   storage. The only difference from a Fake timeline to the first Real
   timeline is in the cursor format. With a Fake timeline, the old
   version is used, i.e. "0000000000000001". Once a Real timeline is
   created, this offset is exposed as "001-0001-000000000000000001".
2. A second timeline is created: this timeline should have a different
   storage and different topic is going to be created by Nakadi for
   this event type.

### Timeline creation

Timeline creation is coordinated through a series of locks and
barriers using Zookeeper. Following we depict an example of what the
ZK datastructure looks like at each step.

#### Initial state

Every time a Nakadi application is launched, it tries to create the
following ZK structure:

```yaml
timelines:
  lock: -                    lock for timeline versions synchronization
  version: {version}      monotonically incremented long value (version of timelines configuration)
  locked_et: -
  nodes:                    nakadi nodes
    node1: {version}    Each nakadi node exposes the version used on this node
    node2: {version}
```

In order to not override the initial structure, due to concurrency,
each instance needs to take the lock `/nakadi/timelines/lock` before
executing.

#### Start timeline creation for et_1

When a new timeline creation is initiated, the first step is to
acquire a lock to update timelines for et_1 by creating an ephemeral
node at `/timelines/locked_et/et_1`.

```yaml
timelines:
  lock: -
  version: 0
  locked_et:
    et_1: -
  nodes:
    node1: 0
    node2: 0
```

#### Notify all Nakadi nodes about change: the version barrier

Next, the instance coordinating the timeline creation bumps the
version node, which all Nakadi instances are listening to changes, so
they are notified when something changes.

```yaml
timelines:
  lock: -
  version: 1       # this is incremented by 1
  locked_et:
    et_1: -
  nodes:
    node1: 0
    node2: 0
```

#### Wait for all nodes to react to the new version

Each Nakadi instance watches the value of the
`/nakadi/timelines/version/` node. When it changes, each instance
checks all locked event types and reacts accordingly, by either
releasing or blocking publishers locally.

Once each instance has updated its local list of locked event types,
it bumps its own version, to let the timeline creator initiator know
that it can proceed.


```yaml
timelines:
  lock: -
  version: 1 
  locked_et:
     et_1: -
  nodes:
    node1: 1       # each instance updates its own version
    node2: 1
```

#### Proceed with timeline creation

Once all instances reacted, the creation proceeds with the initiator
inserting the necessary database entries in the timelines table, and
by snapshotting the latest available offset for the existing
storage. It also creates a topic in the new storage. Be aware that if
a timeline partition has never been used, the offset stored is -1. If
it has a single event, the offset is zero and so on.

#### Remove lock and notify all instances again

Following the same logic for initiating the creation of a timeline,
locks are deleted and version is bumped. All Nakadi instances react by
removing their local locks and switching timeline if necessary.

```yaml
timelines:
  lock: -
  version: 2 
  locked_et:     
  nodes:
    node1: 1
    node2: 1
```


After every instance reacted, it should look like:

```yaml
timelines:
  lock: -
  version: 2 
  locked_et:
  nodes:
    node1: 2       # each instance updates its own version
    node2: 2
```

#### Done

All done here. A new timeline has been created successfully. All
operations are logged so in case you need to debug things, just take a
look at INFO level logs.

<!---
## Cursors

TODO: Describe cursors.
-->
