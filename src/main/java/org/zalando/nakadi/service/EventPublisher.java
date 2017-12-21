package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.EnrichmentException;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.EventValidationException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.PartitioningException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.ValidationError;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Component
public class EventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublisher.class);

    private final NakadiSettings nakadiSettings;

    private final TimelineService timelineService;
    private final EventTypeCache eventTypeCache;
    private final PartitionResolver partitionResolver;
    private final Enrichment enrichment;
    private final TimelineSync timelineSync;
    private final AuthorizationValidator authValidator;

    @Autowired
    public EventPublisher(final TimelineService timelineService,
                          final EventTypeCache eventTypeCache,
                          final PartitionResolver partitionResolver,
                          final Enrichment enrichment,
                          final NakadiSettings nakadiSettings,
                          final TimelineSync timelineSync,
                          final AuthorizationValidator authValidator) {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.nakadiSettings = nakadiSettings;
        this.timelineSync = timelineSync;
        this.authValidator = authValidator;
    }

    public EventPublishResult publish(final String events, final String eventTypeName, final Client client)
            throws NoSuchEventTypeException,
            InternalNakadiException,
            EventTypeTimeoutException,
            AccessDeniedException,
            ServiceTemporarilyUnavailableException {
        return publishInternal(events, eventTypeName, true);
    }

    EventPublishResult publishInternal(final String events,
                                       final String eventTypeName,
                                       final boolean useAuthz)
            throws NoSuchEventTypeException, InternalNakadiException, EventTypeTimeoutException,
            AccessDeniedException, ServiceTemporarilyUnavailableException {

        Closeable publishingCloser = null;
        final List<BatchItem> batch = BatchFactory.from(events);
        try {
            publishingCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            final EventType eventType = eventTypeCache.getEventType(eventTypeName);
            if (useAuthz) {
                authValidator.authorizeEventTypeWrite(eventType);
            }

            validate(batch, eventType);
            partition(batch, eventType);
            enrich(batch, eventType);
            submit(batch, eventType);

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
            LOG.error("error publishing event: {}", e.getProblemMessage());
            return failed(batch);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to wait for timeline switch: {}", e.getMessage());
            throw new EventTypeTimeoutException("Event type is currently in maintenance, please repeat request");
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch: {}", e.getMessage());
            throw new EventTypeTimeoutException("Event type is currently in maintenance, please repeat request");
        } finally {
            try {
                if (publishingCloser != null) {
                    publishingCloser.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type: {}", e.getMessage());
            }
        }
    }

    private void enrich(final List<BatchItem> batch, final EventType eventType) throws EnrichmentException {
        for (final BatchItem batchItem : batch) {
            try {
                batchItem.setStep(EventPublishingStep.ENRICHING);
                enrichment.enrich(batchItem, eventType);
            } catch (EnrichmentException e) {
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

    private void partition(final List<BatchItem> batch, final EventType eventType) throws PartitioningException {
        for (final BatchItem item : batch) {
            item.setStep(EventPublishingStep.PARTITIONING);
            try {
                final String partitionId = partitionResolver.resolvePartition(eventType, item.getEvent());
                item.setPartition(partitionId);
            } catch (final PartitioningException e) {
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, e.getMessage());
                throw e;
            }
        }
    }

    private void validate(final List<BatchItem> batch, final EventType eventType) throws EventValidationException,
            InternalNakadiException, NoSuchEventTypeException {
        for (final BatchItem item : batch) {
            item.setStep(EventPublishingStep.VALIDATING);
            try {
                validateSchema(item.getEvent(), eventType);
                validateEventSize(item);
            } catch (final EventValidationException e) {
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, e.getMessage());
                throw e;
            }
        }
    }

    private void submit(final List<BatchItem> batch, final EventType eventType) throws EventPublishingException {
        final Timeline activeTimeline = timelineService.getActiveTimeline(eventType);
        timelineService.getTopicRepository(eventType).syncPostBatch(activeTimeline.getTopic(), batch);
    }

    private void validateSchema(final JSONObject event, final EventType eventType) throws EventValidationException,
            InternalNakadiException, NoSuchEventTypeException {
        final EventTypeValidator validator = eventTypeCache.getValidator(eventType.getName());
        final Optional<ValidationError> validationError = validator.validate(event);

        if (validationError.isPresent()) {
            throw new EventValidationException(validationError.get());
        }
    }

    private void validateEventSize(final BatchItem item) throws EventValidationException {
        if (item.getEventSize() > nakadiSettings.getEventMaxBytes()) {
            throw new EventValidationException("Event too large: " + item.getEventSize()
                    + " bytes, max size is " + nakadiSettings.getEventMaxBytes() + " bytes");
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
