Timelines
---------

This document covers Timeline internals. It's meant to explain how
timelines work so to help you understand the code and what each part
of it contributes to the overall picture.

# Fake timeline: your first timeline

Previous to timelines, Nakadi would connect to a single Kafka cluster,
which used to be specified in the application yaml file. So this was a
static configuration loaded during boot time. Once timelines were
introduced, in order to easy the migration process and remove special
cases from the implementation, a fake timeline was introduced.

Fake timelines exists only in memory. It's a Timeline which is not
stored in the database. Fake timelines can be seen as a fallback
strategy for when there is no actual timeline for a given event type.

Also, fake timelines use the default storage:the on defined in the
application configuration.

# Timeline creation

Timeline creation is coordinated through a series of locks and
barriers using Zookeeper. Following we depict an example on how ZK
datastructure looks like at each step.

## Initial state

Every time a Nakadi application is launched, it trys to create the
following ZK structure:

```
- timelines
  + - lock                    lock for timeline versions synchronization
  + - version: {version}      monotonically incremented long value (version of timelines configuration)
  + - locked_et:
  + - nodes                   nakadi nodes
    + - {node1}: {version}    Each nakadi node exposes version being used on this node
    + - {node2}: {version}
```

In order not to override it, each instance needs to take the lock
`/nakadi/timelines/lock` before executing.

## Start timeline creation for et_1

When a new timeline creation is initiated, the first step it to
acquire a lock to update timelines for et_1 by creating an ephemeral
node at `/timelines/locked_et/et_1`.

```
- timelines
  + - lock
  + - version: 0
  + - locked_et:
    + - et_1
  + - nodes
    + - node1: 0
    + - node2: 0
```

## Notify all Nakadi nodes about change: the version barrier

Next, it bumps the version node, which all Nakadi instances are
listening for changes, so that they get to be aware that something is
changing with timelines.

```
- timelines
  + - lock
  + - version: 1       # this is incremented by 1
  + - locked_et:
    + - et_1
  + - nodes
    + - node1: 0
    + - node2: 0
```

## Wait for all nodes to react to the new version

Each Nakadi instance checks watches the value of
`/nakadi/timelines/version/` node. When it changes, each instance
checks all locked event types and react accordingly, by either
releasing or blocking publishers.

Once each instace has updated it's local list of locked event types,
it bumps its own version so to let the timeline creator initiator know
that it can proceed.

```
- timelines
  + - lock
  + - version: 1
  + - locked_et:
    + - et_1
  + - nodes
    + - node1: 1       # each instance updates its own version
    + - node2: 1
```

## Proceed with timeline creation

Once all instances reacted accordingly, the creation proceeds with the
initiator inserting the necessary database entries in timelines table
and by snapshotting the latest available offset at the existing
storage. Be aware that if a timeline partition has never been used,
the offset stored is -1. If it has a single event, the offset is zero
and so on.

## Removes lock and notify all instances again

Following the same logic for initiating the creation of a timeline,
locks are deleted and version is bumped. All Nakadi instances react
accordingly by removing their local locks and switching timeline if
necessary.

```
- timelines
  + - lock
  + - version: 2
  + - locked_et:
  + - nodes
    + - node1: 1
    + - node2: 1
```

After every instance reacted, it should look like:

```
- timelines
  + - lock
  + - version: 2
  + - locked_et:
  + - nodes
    + - node1: 2
    + - node2: 2
```

## Done

All done here. A new timeline has been created successfully. All
operations are logged so in case you need to debug things, just take a
look at INFO level logs.

# Cursors

TODO: Describe cursors.
