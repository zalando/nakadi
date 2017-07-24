---
title: Recipes
position: 12
---

# Recipes and Patterns

_This section features patterns on how to use Nakadi and event stream processing in general._


## OverPartitioning

### Problem

Nakadi throughput scales with the number of partitions in an event type. The number of partitions in an event type is fixed &mdash; it can only be configured on create. Scaling throughput by creating a new event type can be tricky though, because the switch-over has to be coordinated between producers and consumers.

You expect a significant increase in throughput over time. How many partitions should you create?

### Solution

Create more partitions then you currently need. Each consumer initially reads from multiple partitions. Increase the number of consumers as throughput increases, until the number of consumers is equal to the number of partitions.

To distribute the workload evenly, make sure that each consumer reads from the same number of partitions. This strategy works best if the number of partitions is a product of small primes:

 - with 6 (= 2 * 3) partitions, you can use 1, 2, 3 or 6 consumers
 - with 8 (= 2 * 2 * 2) partitions, you can use 1, 2, 4 or 8 consumers
 - with 12 (= 2 * 2 * 3) partitions, you can use 1, 2, 3, 4, 6 or 12 consumers

### Discussion

The total number of partitions in a Nakadi cluster is limited. **Start with a single partition**, and employ this pattern only once you are forced to use multiple partitions. Don't over-overpartition, use the lowest sensible number that works. You can always fall back on creating a new event type with more partitions later, if necessary.



## ProcessingPipeline

### Problem

You want to process all events in a given event type, but you have to preserve local (per-partition) ordering of the events.

### Solution

Create a processing pipeline with multiple stages. Each stage consists of a single worker thread, and an inbox (small bounded in-memory list).

Each stage reads events one by one from its inbox, processes them, and puts them in the inbox of the next stage. The first stage reads events from Nakadi instead.

If you want to publish the events to Nakadi after processing, then the last stage can collect them in an internal buffer and post them in batches.

To keep track of Nakadi cursors, you can push them as pseudo-events trough the pipeline. Once the cursor has reached the last stage, all events in the batch must have been processed, so the cursor can be saved.

### Discussion

Using bounded inboxes decouples the stages from each other, creates backpressure between them, and puts an upper limit on the total amount of work-in-progress in the pipeline.

Overall troughput of the pipeline is limited by the stage with the largest average processing time per event. By optimizing this bottleneck, you can optimize the overall throughput. Example: if the slowest stage needs 20ms to process each event, throughput will be lower than 50 events per second.

Each pipeline can consume events from one or more partitions. This setup can be scaled by increasing the number of pipelines running in parallel, up to the number of partitions in the event type.



## More ideas

 - OrderedProducer: strong and weak ordering techniques
 - StatefulConsumer: managing offsets locally
 - UsingPartitions: when to use which partition options and selecting partition keys
 - HighThroughputEventing: approaches to high volume events
 - ConsumerLeaseStealing: redistributing partition workloads in a cluster
 - CausalEventing: events that have causal relationships (happens-before)
 - EventTypeVersioning: approaches to versioning event types
 - SendAnything: approaches to send arbitrary content through Nakadi (incl Avro and Protobufs)
 - ProcessMonitor: a microservice that coordinates/watches other event streams (cf @gregyoung)
