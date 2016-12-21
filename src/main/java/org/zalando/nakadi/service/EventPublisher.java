package org.zalando.nakadi.service;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.*;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.*;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.timeline.StorageWorker;
import org.zalando.nakadi.service.timeline.StorageWorkerFactory;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.ValidationError;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class EventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublisher.class);

    private final TimelineSync timelineSync;
    private final EventTypeCache eventTypeCache;
    private final PartitionResolver partitionResolver;
    private final Enrichment enrichment;
    private final StorageWorkerFactory storageWorkerFactory;
    private final TimelineService timelineService;

    @Autowired
    public EventPublisher(final TimelineSync timelineSync,
                          final EventTypeCache eventTypeCache,
                          final PartitionResolver partitionResolver,
                          final Enrichment enrichment,
                          final StorageWorkerFactory storageWorkerFactory,
                          final TimelineService timelineService) {
        this.storageWorkerFactory = storageWorkerFactory;
        this.timelineSync = timelineSync;
        this.eventTypeCache = eventTypeCache;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.timelineService = timelineService;
    }

    public EventPublishResult publish(final JSONArray events, final String eventTypeName, final Client client)
            throws NoSuchEventTypeException, InternalNakadiException {
        final EventType eventType = eventTypeCache.getEventType(eventTypeName);
        client.checkScopes(eventType.getWriteScopes());

        final List<BatchItem> batch = BatchFactory.from(events);
        try (TimelineSync.CloseableNoException cr = timelineSync.workWithEventType(eventType.getName())) {
            final Timeline timeline = timelineService.getTimeline(eventType);
            final StorageWorker storageWorker = storageWorkerFactory.getWorker(timeline.getStorage());

            validate(batch, eventType);
            partition(batch, eventType, storageWorker.getTopicRepository().listPartitionNames(timeline.getStorageConfiguration()));
            enrich(batch, eventType);
            submit(storageWorker, timeline, batch);

            return ok(batch);
        } catch (final EventValidationException e) {
            LOG.debug("Event validation error: {}", e.getMessage());
            return aborted(EventPublishingStep.VALIDATING, batch);
        } catch (final PartitioningException e) {
            LOG.debug("Event partition error: {}", e.getMessage());
            return aborted(EventPublishingStep.PARTITIONING, batch);
        } catch (final EnrichmentException e) {
            LOG.debug("Event enrichment error: {}", e.getMessage());
            return aborted(EventPublishingStep.ENRICHING, batch);
        } catch (final EventPublishingException e) {
            LOG.error("error publishing event", e);
            return failed(batch);
        } catch (final InterruptedException e) {
            LOG.error("Failed to lock data", e);
            return failed(batch);
        }
    }

    private void enrich(final List<BatchItem> batch, final EventType eventType) throws EnrichmentException {
        for (final BatchItem batchItem : batch) {
            try {
                batchItem.setStep(EventPublishingStep.ENRICHING);
                enrichment.enrich(batchItem, eventType);
            } catch (final EnrichmentException e) {
                batchItem.updateStatusAndDetail(EventPublishingStatus.FAILED, e.getMessage());
                throw e;
            }
        }
    }

    private List<BatchItemResponse> responses(final List<BatchItem> batch) {
        return batch.stream()
                .map(BatchItem::getResponse)
                .collect(Collectors.toList());
    }

    private void partition(final List<BatchItem> batch, final EventType eventType, final List<String> partitions)
            throws PartitioningException {
        for (final BatchItem item : batch) {
            item.setStep(EventPublishingStep.PARTITIONING);
            try {
                final String partitionId = partitionResolver.resolvePartition(eventType, item.getEvent(), partitions);
                item.setPartition(partitionId);
            } catch (final PartitioningException e) {
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, e.getMessage());
                throw e;
            }
        }
    }

    private void validate(final List<BatchItem> batch, final EventType eventType) throws EventValidationException,
            InternalNakadiException {
        for (final BatchItem item : batch) {
            item.setStep(EventPublishingStep.VALIDATING);
            try {
                validateSchema(item.getEvent(), eventType);
            } catch (final EventValidationException e) {
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, e.getMessage());
                throw e;
            }
        }
    }

    private void submit(final StorageWorker storageWorker, final Timeline timeline,
                        final List<BatchItem> batch)
            throws EventPublishingException, InterruptedException {
        // there is no need to group by partition since its already done by kafka client
        storageWorker.getTopicRepository().syncPostBatch(timeline, batch);
    }

    private void validateSchema(final JSONObject event, final EventType eventType) throws EventValidationException,
            InternalNakadiException {
        try {
            final EventTypeValidator validator = eventTypeCache.getValidator(eventType.getName());
            final Optional<ValidationError> validationError = validator.validate(event);

            if (validationError.isPresent()) {
                throw new EventValidationException(validationError.get());
            }
        } catch (final ExecutionException e) {
            throw new InternalNakadiException("Error loading validator", e);
        }
    }

    private EventPublishResult failed(final List<BatchItem> batch) {
        return new EventPublishResult(EventPublishingStatus.FAILED, EventPublishingStep.PUBLISHING, responses(batch));
    }

    private EventPublishResult aborted(final EventPublishingStep step, final List<BatchItem> batch) {
        return new EventPublishResult(EventPublishingStatus.ABORTED, step, responses(batch));
    }

    private EventPublishResult ok(final List<BatchItem> batch) {
        return new EventPublishResult(EventPublishingStatus.SUBMITTED, EventPublishingStep.NONE, responses(batch));
    }
}
