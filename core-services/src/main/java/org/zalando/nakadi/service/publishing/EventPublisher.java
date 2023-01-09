package org.zalando.nakadi.service.publishing;

import com.google.common.collect.ImmutableMap;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.EventPublishingException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.EventValidationException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.exceptions.runtime.PublishEventOwnershipException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.partitioning.EventKeyExtractor;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.JsonSchemaValidator;
import org.zalando.nakadi.validation.ValidationError;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
    private final EventOwnerExtractorFactory eventOwnerExtractorFactory;

    @Autowired
    public EventPublisher(final TimelineService timelineService,
                          final EventTypeCache eventTypeCache,
                          final PartitionResolver partitionResolver,
                          final Enrichment enrichment,
                          final NakadiSettings nakadiSettings,
                          final TimelineSync timelineSync,
                          final AuthorizationValidator authValidator,
                          final EventOwnerExtractorFactory eventOwnerExtractorFactory) {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
        this.partitionResolver = partitionResolver;
        this.enrichment = enrichment;
        this.nakadiSettings = nakadiSettings;
        this.timelineSync = timelineSync;
        this.authValidator = authValidator;
        this.eventOwnerExtractorFactory = eventOwnerExtractorFactory;
    }

    public EventPublishResult publish(final String events, final String eventTypeName)
            throws NoSuchEventTypeException,
            InternalNakadiException,
            EnrichmentException,
            EventTypeTimeoutException,
            AccessDeniedException,
            PublishEventOwnershipException,
            ServiceTemporarilyUnavailableException,
            PartitioningException {
        return processInternal(events, eventTypeName, true, false);
    }

    public EventPublishResult delete(final String events, final String eventTypeName)
            throws NoSuchEventTypeException,
            InternalNakadiException,
            EnrichmentException,
            EventTypeTimeoutException,
            AccessDeniedException,
            PublishEventOwnershipException,
            ServiceTemporarilyUnavailableException,
            PartitioningException {
        return processInternal(events, eventTypeName, true, true);
    }

    EventPublishResult processInternal(final String events,
                                       final String eventTypeName,
                                       final boolean useAuthz,
                                       final boolean delete)
            throws NoSuchEventTypeException, InternalNakadiException, EventTypeTimeoutException,
            AccessDeniedException, ServiceTemporarilyUnavailableException, PublishEventOwnershipException,
            EnrichmentException, PartitioningException {

        Closeable publishingCloser = null;
        final List<BatchItem> batch = BatchFactory.from(events);
        try {
            publishingCloser = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());

            final EventType eventType = eventTypeCache.getEventType(eventTypeName);
            if (useAuthz) {
                authValidator.authorizeEventTypeWrite(eventType);
            }
            validateEventOwnership(eventType, batch);
            validate(batch, eventType, delete);
            partition(batch, eventType);
            assignKey(batch, eventType);
            if (!delete) {
                enrich(batch, eventType);
            }
            submit(batch, eventType, delete);

            return ok(batch);
        } catch (final EventValidationException e) {
            LOG.info(
                    "Event type {} validation error: {}",
                    eventTypeName,
                    Optional.ofNullable(e.getMessage()).map(s -> s.replaceAll("\n", "; ")).orElse(null)
            );
            return aborted(EventPublishingStep.VALIDATING, batch);
        } catch (final PartitioningException e) {
            LOG.debug("Event partition error: {}", e.getMessage());
            return aborted(EventPublishingStep.PARTITIONING, batch);
        } catch (final EnrichmentException e) {
            LOG.debug("Event enrichment error: {}", e.getMessage());
            return aborted(EventPublishingStep.ENRICHING, batch);
        } catch (final PublishEventOwnershipException e) {
            LOG.debug("Event ownership error: {}", e.getMessage());
            return aborted(EventPublishingStep.VALIDATING, batch);
        } catch (final EventPublishingException e) {
            LOG.error("error publishing event", e);
            return failed(batch);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeTimeoutException("Event type is currently in maintenance, please repeat request");
        } catch (final TimeoutException e) {
            LOG.error("Failed to wait for timeline switch", e);
            throw new EventTypeTimeoutException("Event type is currently in maintenance, please repeat request");
        } finally {
            try {
                if (publishingCloser != null) {
                    publishingCloser.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type", e);
            }
        }
    }

    private void enrich(final List<BatchItem> batch, final EventType eventType)
            throws EnrichmentException {

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

        final Function<BatchItem, List<String>> keyExtractor = EventKeyExtractor.partitionKeysFromBatchItem(eventType);

        final List<String> orderedPartitions = eventTypeCache.getOrderedPartitions(eventType.getName());

        for (final BatchItem item : batch) {
            item.setStep(EventPublishingStep.PARTITIONING);
            try {
                item.setPartitionKeys(keyExtractor.apply(item));
                item.setPartition(partitionResolver.resolvePartition(eventType, item, orderedPartitions));
            } catch (final PartitioningException e) {
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, e.getMessage());
                throw e;
            }
        }
    }

    private static void assignKey(final List<BatchItem> batch, final EventType eventType) {
        final Function<BatchItem, String> kafkaKeyExtractor = EventKeyExtractor.kafkaKeyFromBatchItem(eventType);
        for (final BatchItem item : batch) {
            item.setEventKey(kafkaKeyExtractor.apply(item));
        }
    }

    private void validateEventOwnership(final EventType eventType, final List<BatchItem> batchItems) {
        final EventOwnerExtractor extractor = eventOwnerExtractorFactory.createExtractor(eventType);
        if (null == extractor) {
            return;
        }

        for (final BatchItem item : batchItems) {
            item.setStep(EventPublishingStep.VALIDATING);

            final EventOwnerHeader owner = extractor.extractEventOwner(item.getEvent());
            item.setOwner(owner);

            try {
                authValidator.authorizeEventWrite(item);
            } catch (AccessDeniedException e) {
                item.updateStatusAndDetail(EventPublishingStatus.FAILED, e.explain());
                throw new PublishEventOwnershipException(e.explain(), e);
            }
        }
    }

    private void validate(final List<BatchItem> batch, final EventType eventType, final boolean delete)
            throws EventValidationException, InternalNakadiException, NoSuchEventTypeException {

        final Tracer.SpanBuilder validationSpan = TracingService.buildNewSpan("validation")
                .withTag("event_type", eventType.getName());

        try (Closeable ignored = TracingService.withActiveSpan(validationSpan)) {

            if (delete && eventType.getCleanupPolicy() == CleanupPolicy.DELETE) {
                throw new EventValidationException("It is not allowed to delete events from non compacted event type");
            }
            for (final BatchItem item : batch) {
                item.setStep(EventPublishingStep.VALIDATING);
                try {
                    if (!delete) {
                        validateSchema(item.getEvent(), eventType);
                    }
                    validateEventSize(item);
                } catch (final EventValidationException e) {
                    item.updateStatusAndDetail(EventPublishingStatus.FAILED, e.getMessage());
                    if (eventType.getCategory() != EventCategory.UNDEFINED) {
                        TracingService.setErrorFlag();
                        TracingService.log(ImmutableMap.of(
                                "event.id", item.getEvent().getJSONObject("metadata").getString("eid"),
                                TracingService.ERROR_DESCRIPTION, e.getMessage()));
                    }
                    throw e;
                }
            }
        } catch (final IOException ioe) {
            throw new InternalNakadiException("Error closing active span scope", ioe);
        }
    }

    private void submit(
            final List<BatchItem> batch, final EventType eventType, final boolean delete)
        throws EventPublishingException, InternalNakadiException {
        final Timeline activeTimeline = timelineService.getActiveTimeline(eventType);
        final String topic = activeTimeline.getTopic();

        final Tracer.SpanBuilder topicRepositorySpan = TracingService.buildNewSpan("get_topic_repository")
                .withTag(Tags.MESSAGE_BUS_DESTINATION.getKey(), topic);
        final TopicRepository topicRepository;
        try (Closeable ignored = TracingService.withActiveSpan(topicRepositorySpan)) {
            topicRepository = timelineService.getTopicRepository(eventType);
        } catch (final IOException ioe) {
            throw new InternalNakadiException("Error closing active span scope", ioe);
        }

        final Span publishingSpan = TracingService.buildNewSpan("publishing_to_kafka")
                .withTag(Tags.MESSAGE_BUS_DESTINATION.getKey(), topic)
                .start();
        try (Closeable ignored = TracingService.activateSpan(publishingSpan)) {
            topicRepository.syncPostBatch(topic, batch, eventType.getName(), delete);
        } catch (final EventPublishingException epe) {
            publishingSpan.log(epe.getMessage());
            throw epe;
        } catch (final IOException ioe) {
            throw new InternalNakadiException("Error closing active span scope", ioe);
        } finally {
            publishingSpan.finish();
        }
    }

    private void validateSchema(final JSONObject event, final EventType eventType)
            throws EventValidationException, InternalNakadiException, NoSuchEventTypeException {

        final JsonSchemaValidator validator = eventTypeCache.getValidator(eventType.getName());
        final Optional<ValidationError> validationError = validator.validate(event);
        if (validationError.isPresent()) {
            throw new EventValidationException(validationError.get().getMessage());
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
