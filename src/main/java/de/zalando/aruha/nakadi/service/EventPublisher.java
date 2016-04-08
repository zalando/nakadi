package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.BatchItemResponse;
import de.zalando.aruha.nakadi.domain.EventPublishResult;
import de.zalando.aruha.nakadi.domain.EventPublishingStatus;
import de.zalando.aruha.nakadi.domain.EventPublishingStep;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.enrichment.Enrichment;
import de.zalando.aruha.nakadi.exceptions.EnrichmentException;
import de.zalando.aruha.nakadi.exceptions.EventPublishingException;
import de.zalando.aruha.nakadi.exceptions.EventValidationException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import de.zalando.aruha.nakadi.partitioning.PartitionResolver;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.validation.EventTypeValidator;
import de.zalando.aruha.nakadi.validation.ValidationError;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class EventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(EventPublisher.class);

    private final TopicRepository topicRepository;
    private final EventTypeCache eventTypeCache;
    private final PartitionResolver partitionResolver;
    private final Enrichment enrichment;

    public EventPublisher(final TopicRepository topicRepository,
                          final EventTypeCache eventTypeCache,
                          final PartitionResolver partitionResolver,
                          final Enrichment enrichment) {
        this.topicRepository = topicRepository;
        this.eventTypeCache = eventTypeCache;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
    }

    public EventPublishResult publish(final JSONArray events, final String eventTypeName) throws NoSuchEventTypeException,
            InternalNakadiException {
        final EventType eventType = eventTypeCache.getEventType(eventTypeName);
        final List<BatchItem> batch = initBatch(events);

        try {
            validate(batch, eventType);
            enrich(batch, eventType);
            partition(batch, eventType);
            submit(batch, eventType);

            return ok(batch);
        } catch (final EventValidationException e) {
            LOG.debug("Event validation error: {}", e.getMessage());
            return aborted(EventPublishingStep.VALIDATING, batch);
        } catch (EnrichmentException e) {
            LOG.debug("Event enrichment error: {}", e.getMessage());
            return aborted(EventPublishingStep.ENRICHING, batch);
        } catch (final PartitioningException e) {
            LOG.debug("Event partition error: {}", e.getMessage());
            return aborted(EventPublishingStep.PARTITIONING, batch);
        } catch (final EventPublishingException e) {
            LOG.error("error publishing event", e);
            return failed(batch);
        }
    }

    private void enrich(final List<BatchItem> batch, final EventType eventType) throws EnrichmentException {
        for (BatchItem item : batch) {
            try {
                item.setStep(EventPublishingStep.ENRICHING);
                enrichment.enrich(item.getEvent(), eventType);
            } catch (EnrichmentException e) {
                item.setPublishingStatus(EventPublishingStatus.FAILED);
                item.setDetail(e.getMessage());

                throw e;
            }
        }
    }

    private List<BatchItemResponse> responses(final List<BatchItem> batch) {
        return batch.stream()
                .map(BatchItem::getResponse)
                .collect(Collectors.toList());
    }

    private List<BatchItem> initBatch(final JSONArray events) {
        final List<BatchItem> batch = new ArrayList<>(events.length());
        for (int i = 0; i < events.length(); i++) {
            batch.add(new BatchItem(events.getJSONObject(i)));
        }

        return batch;
    }

    private void partition(final List<BatchItem> batch, final EventType eventType) throws PartitioningException {
        for (final BatchItem item : batch) {
            item.setStep(EventPublishingStep.PARTITIONING);
            try {
                final String partitionId = partitionResolver.resolvePartition(eventType, item.getEvent());
                item.setPartition(partitionId);
            } catch (final PartitioningException e) {
                item.setPublishingStatus(EventPublishingStatus.FAILED);
                item.setDetail(e.getMessage());
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
                item.setPublishingStatus(EventPublishingStatus.FAILED);
                item.setDetail(e.getMessage());
                throw e;
            }
        }
    }

    private void submit(final List<BatchItem> batch, final EventType eventType) throws EventPublishingException {
        // there is no need to group by partition since its already done by kafka client
        topicRepository.syncPostBatch(eventType.getName(), batch);
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
