package org.zalando.nakadi.service.kpi;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.nakadi.util.FeatureToggleService;
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
public final class NakadiPublisher {

    private final static Logger LOG = LoggerFactory.getLogger(NakadiPublisher.class);
    private final EventPublisher eventPublisher;
    private final Map<String, BlockingQueue<JSONObject>> eventTypeEvents;
    private final ExecutorService executorService;
    private final UUIDGenerator uuidGenerator;
    private final FeatureToggleService featureToggleService;

    private final long batchCollectionTimeout;
    private final int batchSize;
    private final long pollTimeout;
    private final int eventsQueueSize;
    private final long scheduleTimeout;

    @Autowired
    public NakadiPublisher(final EventPublisher eventPublisher,
                           final UUIDGenerator uuidGenerator,
                           final FeatureToggleService featureToggleService,
                           @Value("${nakadi.kpi.batch-collection-timeout:1000}") final long batchCollectionTimeout,
                           @Value("${nakadi.kpi.batch-size:3}") final int batchSize,
                           @Value("${nakadi.kpi.workers:1}") final int workers,
                           @Value("${nakadi.kpi.poll-timeout:100}") final long pollTimeout,
                           @Value("${nakadi.kpi.events-queue-size:100}") final int eventsQueueSize,
                           @Value("${nakadi.kpi.schedule-timeout:5000}") final long scheduleTimeout) {
        this.eventPublisher = eventPublisher;
        this.uuidGenerator = uuidGenerator;
        this.featureToggleService = featureToggleService;
        this.batchCollectionTimeout = batchCollectionTimeout;
        this.batchSize = batchSize;
        this.pollTimeout = pollTimeout;
        this.eventsQueueSize = eventsQueueSize;
        this.scheduleTimeout = scheduleTimeout;
        this.eventTypeEvents = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(workers);
        this.executorService.submit(() -> scheduleEventsBatchesSend());
    }

    private void scheduleEventsBatchesSend() {
        LOG.trace("Schedule events collection on {}", Thread.currentThread().getName());
        if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION)) {
            for (final String etName : eventTypeEvents.keySet()) {
                executorService.submit(() -> sendEventBatch(etName));
            }
        }
        try {
            Thread.sleep(scheduleTimeout);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executorService.submit(() -> scheduleEventsBatchesSend());
    }

    private void sendEventBatch(final String etName) {
        LOG.trace("Collecting batch for {} on {}", etName, Thread.currentThread().getName());
        try {
            final BlockingQueue<JSONObject> events = eventTypeEvents.get(etName);
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
                eventPublisher.publish(jsonArray.toString(), etName, null);
                LOG.trace("Published batch of {} to {}", eventsCount, etName);
            } catch (final Exception e) {
                LOG.error("Error occurred while publishing events to {}, {}", etName, e.getMessage(), e);
            }
        } finally {
            if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.KPI_COLLECTION)) {
                executorService.submit(() -> sendEventBatch(etName));
            }
        }
    }

    public final void enrichAndSubmit(final JSONObject event, final String etName) {
        final JSONObject metadata = new JSONObject()
                .put("occurred_at", Instant.now())
                .put("eid", uuidGenerator.randomUUID());
        event.put("metadata", metadata);
        final BlockingQueue<JSONObject> events =
                eventTypeEvents.computeIfAbsent(etName, etn -> new ArrayBlockingQueue<>(eventsQueueSize));
        if (!events.offer(event)) {
            LOG.warn("Rejecting events to be queued for {} due to queue overload", etName);
        }
    }

}
