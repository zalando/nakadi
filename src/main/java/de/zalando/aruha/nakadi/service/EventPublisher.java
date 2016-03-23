package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.BatchItem;
import de.zalando.aruha.nakadi.domain.BatchItemResponse;
import de.zalando.aruha.nakadi.domain.EventPublishResult;
import de.zalando.aruha.nakadi.domain.EventPublishingStatus;
import de.zalando.aruha.nakadi.domain.EventPublishingStep;
import de.zalando.aruha.nakadi.domain.EventType;
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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class EventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(EventPublisher.class);

    private final TopicRepository topicRepository;
    private final EventTypeCache cache;
    private final PartitionResolver partitionResolver;

    public EventPublisher(final TopicRepository topicRepository,
                         final EventTypeCache cache,
                         final PartitionResolver partitionResolver) {
        this.topicRepository = topicRepository;
        this.cache = cache;
        this.partitionResolver = partitionResolver;
    }

    public EventPublishResult publish(JSONArray events, String eventTypeName) throws NoSuchEventTypeException,
            InternalNakadiException {
        final EventType eventType = cache.getEventType(eventTypeName);
        final List<BatchItem> batch = initBatch(events);

        try {
            validate(batch, eventType);
            partition(batch, eventType);
            submit(batch, eventType);

            return ok(batch);
        } catch (final EventValidationException e) {
            LOG.debug("Event validation error: {}", e.getMessage());
            return aborted(EventPublishingStep.VALIDATION, batch);
        } catch (final PartitioningException e) {
            LOG.debug("Event partitioning error: {}", e.getMessage());
            return aborted(EventPublishingStep.PARTITIONING, batch);
        } catch (final EventPublishingException e) {
            LOG.error("error publishing event", e);
            return failed(batch);
        }
    }

    private List<BatchItemResponse> responses(List<BatchItem> batch) {
        return batch.stream()
                .map(BatchItem::getResponse)
                .collect(Collectors.toList());
    }

    private List<BatchItem> initBatch(JSONArray events) {
        List<BatchItem> batch = new ArrayList<>();
        for (int i = 0; i < events.length(); i++) {
            batch.add(new BatchItem(events.getJSONObject(i)));
        }

        return batch;
    }

    private void partition(List<BatchItem> batch, EventType eventType) throws PartitioningException {
        for (BatchItem item : batch) {
            item.setStep(EventPublishingStep.PARTITIONING);
            try {
                String partitionId = partitionResolver.resolvePartition(eventType, item.getEvent());
                item.setPartition(partitionId);
            } catch (PartitioningException e) {
                item.setPublishingStatus(EventPublishingStatus.FAILED);
                item.setDetail(e.getMessage());
                throw e;
            }
        }
    }

    private void validate(List<BatchItem> batch, EventType eventType) throws EventValidationException, InternalNakadiException {
        for (BatchItem item : batch) {
            item.setStep(EventPublishingStep.VALIDATION);
            try {
                validateSchema(item.getEvent(), eventType);
            } catch (EventValidationException e) {
                item.setPublishingStatus(EventPublishingStatus.FAILED);
                item.setDetail(e.getMessage());
                throw e;
            }
        }
    }

    private void submit(List<BatchItem> batch, EventType eventType) throws EventPublishingException {
        // there is no need to group by partition since its already done by kafka client
        topicRepository.syncPostBatch(eventType.getName(), batch);
    }

    private void validateSchema(final JSONObject event, final EventType eventType) throws EventValidationException, InternalNakadiException {
        try {
            final EventTypeValidator validator = cache.getValidator(eventType.getName());
            final Optional<ValidationError> validationError = validator.validate(event);

            if (validationError.isPresent()) {
                throw new EventValidationException(validationError.get());
            }
        } catch (ExecutionException e) {
            LOG.error("Error loading validator", e);
            throw new InternalNakadiException("Error loading validator", e);
        }
    }

    private EventPublishResult failed(List<BatchItem> batch) {
        return new EventPublishResult(EventPublishingStatus.FAILED, EventPublishingStep.PUBLISHING, responses(batch));
    }

    private EventPublishResult aborted(EventPublishingStep step, List<BatchItem> batch) {
        return new EventPublishResult(EventPublishingStatus.ABORTED, step, responses(batch));
    }

    private EventPublishResult ok(List<BatchItem> batch) {
        return new EventPublishResult(EventPublishingStatus.SUBMITTED, EventPublishingStep.NONE, responses(batch));
    }
}
