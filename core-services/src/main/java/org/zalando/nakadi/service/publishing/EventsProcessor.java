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

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class EventsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(EventsProcessor.class);
    private final EventPublisher eventPublisher;
    private final Map<String, BlockingQueue<JSONObject>> eventTypeEvents;
    private final ExecutorService executorService;
    private final UUIDGenerator uuidGenerator;

    private final long batchCollectionTimeout;
    private final int batchSize;
    private final long pollTimeout;
    private final int eventsQueueSize;

    @Autowired
    public EventsProcessor(final EventPublisher eventPublisher,
                           final UUIDGenerator uuidGenerator,
                           @Value("${nakadi.kpi.config.batch-collection-timeout}") final long batchCollectionTimeout,
                           @Value("${nakadi.kpi.config.batch-size}") final int batchSize,
                           @Value("${nakadi.kpi.config.workers}") final int workers,
                           @Value("${nakadi.kpi.config.poll-timeout}") final long pollTimeout,
                           @Value("${nakadi.kpi.config.events-queue-size}") final int eventsQueueSize) {
        this.eventPublisher = eventPublisher;
        this.uuidGenerator = uuidGenerator;
        this.batchCollectionTimeout = batchCollectionTimeout;
        this.batchSize = batchSize;
        this.pollTimeout = pollTimeout;
        this.eventsQueueSize = eventsQueueSize;
        this.eventTypeEvents = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(workers);
    }

    private void sendEventBatch(final String etName) {
        LOG.trace("Collecting batch for {} on {}", etName, Thread.currentThread().getName());
        try {
            final BlockingQueue<JSONObject> events = eventTypeEvents.get(etName);
            if (events == null) {
                return;
            }

            final long finishAt = System.currentTimeMillis() + batchCollectionTimeout;
            int eventsCount = 0;
            JSONArray jsonArray = null;
            while (eventsCount != batchSize && System.currentTimeMillis() < finishAt) {
                try {
                    final JSONObject event = events.poll(pollTimeout, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        if (jsonArray == null) {
                            jsonArray = new JSONArray();
                        }
                        jsonArray.put(event);
                        eventsCount++;
                    }
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                if (eventsCount == 0) {
                    LOG.trace("No kpi events send to {}", etName);
                    return;
                }
                eventPublisher.processInternal(jsonArray.toString(), etName, false, null, false);
                LOG.trace("Published batch of {} to {}", eventsCount, etName);
            } catch (final Exception e) {
                LOG.error("Error occurred while publishing events to {}, {}", etName, e.getMessage(), e);
            }
        } finally {
            executorService.submit(() -> sendEventBatch(etName));
        }
    }

    public void enrichAndSubmit(final String etName, final JSONObject event) {
        final JSONObject metadata = new JSONObject()
                .put("occurred_at", Instant.now())
                .put("eid", uuidGenerator.randomUUID())
                .put("flow_id", FlowIdUtils.peek());
        event.put("metadata", metadata);

        final BlockingQueue<JSONObject> events =
                eventTypeEvents.computeIfAbsent(etName, etn -> {
                    LOG.trace("Schedule events collection");
                    executorService.submit(() -> sendEventBatch(etName));
                    return new ArrayBlockingQueue<>(eventsQueueSize);
                });

        if (!events.offer(event)) {
            LOG.warn("Rejecting events to be queued for {} due to queue overload", etName);
        }
    }

}
