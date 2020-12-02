package org.zalando.nakadi.service.publishing;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class EventsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(EventsProcessor.class);

    private final EventPublisher eventPublisher;
    private final ExecutorService executorService;
    private final UUIDGenerator uuidGenerator;

    private final long batchCollectionTimeout;
    private final int maxBatchSize;

    private final BlockingQueue<EventToPublish> eventsQueue;
    private final Thread dispatcherThread;

    private static class EventToPublish {
        private final String eventType;
        private final JSONObject object;

        public EventToPublish(final String eventType, final JSONObject object) {
            this.eventType = eventType;
            this.object = object;
        }
    }

    @Autowired
    public EventsProcessor(final EventPublisher eventPublisher,
                           final UUIDGenerator uuidGenerator,
                           @Value("${nakadi.kpi.config.batch-collection-timeout}") final long batchCollectionTimeout,
                           @Value("${nakadi.kpi.config.batch-size}") final int maxBatchSize,
                           @Value("${nakadi.kpi.config.workers}") final int workers,
                           @Value("${nakadi.kpi.config.events-queue-size}") final int eventsQueueSize) {
        this.eventPublisher = eventPublisher;
        this.uuidGenerator = uuidGenerator;
        this.batchCollectionTimeout = batchCollectionTimeout;
        this.maxBatchSize = maxBatchSize;

        this.executorService = Executors.newFixedThreadPool(workers);
        this.eventsQueue = new ArrayBlockingQueue<>(eventsQueueSize);
        this.dispatcherThread = new Thread(this::dispatch, "processor-dispatch");
    }

    @PostConstruct
    public void start() {
        dispatcherThread.start();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        dispatcherThread.interrupt();
        dispatcherThread.join();
        executorService.shutdown();
    }

    private static class BatchedRequest {
        private final String eventType;
        private final long finishCollectionAt;
        private final JSONArray data;
        private int size = 0;

        public BatchedRequest(final String eventType, final long finishCollectionAt) {
            this.eventType = eventType;
            this.finishCollectionAt = finishCollectionAt;
            this.data = new JSONArray();
        }

        public int add(final JSONObject obj) {
            this.data.put(obj);
            return ++size;
        }

        @Override
        public String toString() {
            return "Batch{et=" + eventType + ",size:" + size + "}";
        }
    }

    private void scheduleSendBatchedRequest(final BatchedRequest req) {
        executorService.submit(() -> {
            try {
                eventPublisher.processInternal(req.data.toString(), req.eventType, false, null, false);
            } catch (final RuntimeException ex) {
                LOG.info("Failed to send single batch for unknown reason", ex);
            }
        });
    }

    private void dispatch() {
        final Map<String, BatchedRequest> batchesBeingAssembled = new HashMap<>();
        try {
            long nextTimeCheck = System.currentTimeMillis() + batchCollectionTimeout;
            while (true) {
                long currentTime = System.currentTimeMillis();
                final EventToPublish data = eventsQueue.poll(
                        Math.max(nextTimeCheck - currentTime, 1), TimeUnit.MILLISECONDS);
                currentTime = System.currentTimeMillis();
                boolean batchWasSent = false;
                if (data != null) {
                    BatchedRequest batch = batchesBeingAssembled.get(data.eventType);
                    if (null == batch) {
                        batch = new BatchedRequest(data.eventType, currentTime + batchCollectionTimeout);
                        batchesBeingAssembled.put(data.eventType, batch);
                    }
                    if (batch.add(data.object) >= maxBatchSize) {
                        scheduleSendBatchedRequest(batch);
                        batchesBeingAssembled.remove(batch.eventType);
                        currentTime = System.currentTimeMillis();
                        batchWasSent = true;
                    }
                }
                if (batchWasSent || currentTime > nextTimeCheck) {
                    nextTimeCheck = currentTime + batchCollectionTimeout;
                    for (Map.Entry<String, BatchedRequest> entry : batchesBeingAssembled.entrySet()) {
                        if (entry.getValue().finishCollectionAt < currentTime) {
                            scheduleSendBatchedRequest(entry.getValue());
                            batchesBeingAssembled.remove(entry.getKey());
                        } else if (nextTimeCheck > entry.getValue().finishCollectionAt) {
                            nextTimeCheck = entry.getValue().finishCollectionAt;
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            LOG.info("Was interrupted while dispatching batches");
        }
        // now we have a lot of stuff to send
        EventToPublish taken;
        while (null != (taken = eventsQueue.poll())) {
            BatchedRequest batchedRequest = batchesBeingAssembled.get(taken.eventType);
            if (null == batchedRequest) {
                batchedRequest = new BatchedRequest(taken.eventType, System.currentTimeMillis());
                batchesBeingAssembled.put(taken.eventType, batchedRequest);
            }
            if (batchedRequest.add(taken.object) >= maxBatchSize) {
                scheduleSendBatchedRequest(batchedRequest);
                batchesBeingAssembled.remove(batchedRequest.eventType);
            }
        }
        for (final BatchedRequest req : batchesBeingAssembled.values()) {
            scheduleSendBatchedRequest(req);
            batchesBeingAssembled.remove(req.eventType);
        }
    }

    public void enrichAndSubmit(final String etName, final JSONObject event) {
        final JSONObject metadata = new JSONObject()
                .put("occurred_at", Instant.now())
                .put("eid", uuidGenerator.randomUUID())
                .put("flow_id", FlowIdUtils.peek());
        event.put("metadata", metadata);

        if (!eventsQueue.offer(new EventToPublish(etName, event))) {
            LOG.warn("Rejecting events to be queued for {} due to queue overload", etName);
        }
    }

}
