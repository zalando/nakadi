package org.zalando.nakadi.service.publishing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordMetadata;

import java.util.List;

@Component
@Qualifier("avro-publisher")
public class AvroEventProcessor extends EventsProcessor<NakadiRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(EventsProcessor.class);

    private final AvroEventPublisher avroEventPublisher;

    public AvroEventProcessor(
            final AvroEventPublisher avroEventPublisher,
            @Value("${nakadi.kpi.config.batch-collection-timeout}") final long batchCollectionTimeout,
            @Value("${nakadi.kpi.config.batch-size}") final int maxBatchSize,
            @Value("${nakadi.kpi.config.workers}") final int workers,
            @Value("${nakadi.kpi.config.batch-queue:100}") final int maxBatchQueue,
            @Value("${nakadi.kpi.config.events-queue-size}") final int eventsQueueSize) {
        super(batchCollectionTimeout, maxBatchSize, workers, maxBatchQueue, eventsQueueSize);
        this.avroEventPublisher = avroEventPublisher;
    }

    @Override
    public void sendEvents(final String etName, final List<NakadiRecord> events) {
        try {
            final List<NakadiRecordMetadata> nakadiRecordMetadata =
                    avroEventPublisher.publishAvro(events);
            if (!nakadiRecordMetadata.isEmpty()) {
                LOG.warn("failed to publish events to {} {}",
                        etName, nakadiRecordMetadata.get(0).getException());
            }
        } catch (final RuntimeException ex) {
            LOG.error("failed to send single batch for unknown reason", ex);
        }
    }

}
