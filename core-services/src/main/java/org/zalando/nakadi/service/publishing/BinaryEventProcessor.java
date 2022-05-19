package org.zalando.nakadi.service.publishing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.service.publishing.check.Check;

import java.util.List;

import static org.zalando.nakadi.domain.NakadiRecordResult.Status;

@Component
@Qualifier("avro-publisher")
public class BinaryEventProcessor extends EventsProcessor<NakadiRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryEventProcessor.class);

    private final BinaryEventPublisher binaryEventPublisher;
    private final EventTypeCache eventTypeCache;

    private final List<Check> prePublishingChecks;

    public BinaryEventProcessor(
            final BinaryEventPublisher binaryEventPublisher,
            final EventTypeCache eventTypeCache,
            @Qualifier("internal-publishing-checks") final List<Check> prePublishingChecks,
            @Value("${nakadi.kpi.config.batch-collection-timeout}") final long batchCollectionTimeout,
            @Value("${nakadi.kpi.config.batch-size}") final int maxBatchSize,
            @Value("${nakadi.kpi.config.workers}") final int workers,
            @Value("${nakadi.kpi.config.batch-queue:100}") final int maxBatchQueue,
            @Value("${nakadi.kpi.config.events-queue-size}") final int eventsQueueSize) {
        super(batchCollectionTimeout, maxBatchSize, workers, maxBatchQueue, eventsQueueSize);
        this.binaryEventPublisher = binaryEventPublisher;
        this.eventTypeCache = eventTypeCache;
        this.prePublishingChecks = prePublishingChecks;

        if (prePublishingChecks.isEmpty()) {
            // Safeguard against silent failure one spring injecting empty list
            throw new RuntimeException("Checks should not be empty");
        }
    }

    @Override
    public void sendEvents(final String etName, final List<NakadiRecord> events) {
        try {
            final EventType eventType = eventTypeCache.getEventType(etName);
            final List<NakadiRecordResult> eventRecordMetadata =
                    binaryEventPublisher.publishWithChecks(eventType, events, prePublishingChecks);
            eventRecordMetadata.stream()
                    .filter(nrr -> nrr.getStatus() != Status.SUCCEEDED)
                    .forEach(nrr ->
                            LOG.warn("failed to publish events to {} status {} exception {}",
                                    etName, nrr.getStatus(), nrr.getException()));
        } catch (final RuntimeException ex) {
            LOG.error("failed to send single batch for unknown reason", ex);
        }
    }

}
