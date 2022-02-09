package org.zalando.nakadi.service.publishing;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Qualifier("json-publisher")
public class JsonEventProcessor extends EventsProcessor<JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonEventProcessor.class);

    private final EventPublisher eventPublisher;

    public JsonEventProcessor(
            final EventPublisher eventPublisher,
            @Value("${nakadi.kpi.config.batch-collection-timeout}") final long batchCollectionTimeout,
            @Value("${nakadi.kpi.config.batch-size}") final int maxBatchSize,
            @Value("${nakadi.kpi.config.workers}") final int workers,
            @Value("${nakadi.kpi.config.batch-queue:100}") final int maxBatchQueue,
            @Value("${nakadi.kpi.config.events-queue-size}") final int eventsQueueSize) {
        super(batchCollectionTimeout, maxBatchSize, workers, maxBatchQueue, eventsQueueSize);
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void sendEvents(final String etName, final List<JSONObject> events) {
        try {
            // sending events batch with disabled authz check
            eventPublisher.processInternal(new JSONArray(events).toString(), etName, false, false);
        } catch (final RuntimeException ex) {
            LOG.error("Failed to send single batch for unknown reason", ex);
        }
    }
}
