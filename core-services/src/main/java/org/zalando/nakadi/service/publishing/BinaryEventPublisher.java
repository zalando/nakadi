package org.zalando.nakadi.service.publishing;

import com.google.common.collect.Lists;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordMetadata;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Service
public class BinaryEventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryEventPublisher.class);

    private final TimelineService timelineService;
    private final EventTypeCache eventTypeCache;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;

    private final NakadiBatchMapper mapper;
    private final EventOwnerExtractorFactory eventOwnerExtractorFactory;
    private final AuthorizationValidator authValidator;


    @Autowired
    public BinaryEventPublisher(
            final TimelineService timelineService,
            final EventTypeCache eventTypeCache,
            final TimelineSync timelineSync,
            final NakadiSettings nakadiSettings,
            final NakadiBatchMapper mapper,
            final EventOwnerExtractorFactory eventOwnerExtractorFactory,
            final AuthorizationValidator authValidator) {
        this.timelineService = timelineService;
        this.eventTypeCache = eventTypeCache;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.mapper = mapper;
        this.eventOwnerExtractorFactory = eventOwnerExtractorFactory;
        this.authValidator = authValidator;
    }

    public List<NakadiRecordMetadata> publish(final String eventTypeName,
                                              final List<NakadiRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("events have to be present when publishing");
        }

        Closeable publishingCloser = null;
        try {
            // publish under timeline lock
            publishingCloser = timelineSync.workWithEventType(
                    eventTypeName,
                    nakadiSettings.getTimelineWaitTimeoutMs());
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);
            final Timeline activeTimeline = timelineService.getActiveTimeline(eventType);
            final String topic = activeTimeline.getTopic();
            final Span publishingSpan = TracingService
                    .buildNewSpan("publishing_to_kafka")
                    .withTag(Tags.MESSAGE_BUS_DESTINATION.getKey(), topic)
                    .withTag("event_type", eventTypeName)
                    .withTag("type", "binary")
                    .start();
            try (Closeable ignored = TracingService.activateSpan(publishingSpan)) {
                return timelineService.getTopicRepository(eventType).sendEvents(topic, records);
            } catch (final IOException ioe) {
                throw new InternalNakadiException("Error closing active span scope", ioe);
            } finally {
                publishingSpan.finish();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InternalNakadiException("Failed to wait for timeline switch", e);
        } catch (final TimeoutException e) {
            throw new InternalNakadiException("Failed to wait for timeline switch", e);
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

    /**
     * from {@link EventPublisher#processInternal}
     * <p>
     * steps to add
     * 1. validateEventOwnership
     * 2. validate
     * 3. partition
     * 4. setEventKey
     * 5. enrich
     * 6. submit
     */
    private final List<Step> steps = Lists.newArrayList(
            new EventCompliance());

    public EventPublishResult publish(final String eventTypeName, final byte[] batch) {
        try {
            final List<NakadiRecord> recordsBatch = mapper.map(batch);
            final EventType eventType = eventTypeCache.getEventType(eventTypeName);

            for (final Step step : steps) {
                final EventPublishResult res = step.validate(eventType, recordsBatch);
                if (res != null) {
                    return res;
                }
            }

            return new EventPublishResult(
                    EventPublishingStatus.SUBMITTED,
                    EventPublishingStep.NONE,
                    null);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private interface Step {
        EventPublishResult validate(EventType eventType,
                                    List<NakadiRecord> records);

        default List<BatchItemResponse> prepareResponses(
                final List<NakadiRecord> records,
                final NakadiRecord failedRecord,
                final EventPublishingStep step,
                final String error) {
            return records.stream().map(nakadiRecord -> {
                final BatchItemResponse resp = new BatchItemResponse()
                        .setEid(nakadiRecord.getEventMetadata().get("eid").toString())
                        .setStep(step)
                        .setDetail(error);
                if (failedRecord == nakadiRecord) {
                    resp.setPublishingStatus(EventPublishingStatus.FAILED);
                }
                return resp;
            }).collect(Collectors.toList());
        }

    }

    private class EventCompliance implements Step {

        @Override
        public EventPublishResult validate(final EventType eventType,
                                           final List<NakadiRecord> records) {

            // throws exception
            authValidator.authorizeEventTypeWrite(eventType);

            for (final NakadiRecord record : records) {
                final String recordEventType = record.getEventMetadata()
                        .get("event_type").toString();

                if (!eventType.getName().equals(recordEventType)) {
                    return new EventPublishResult(
                            EventPublishingStatus.ABORTED,
                            EventPublishingStep.VALIDATING,
                            prepareResponses(records, record,
                                    EventPublishingStep.VALIDATING, "event type does not match"));
                }

                // todo should be amongst active schemas, fix after schema registry
//                final String recordSchemaVersion = record.getEventMetadata()
//                        .get("schema_version").toString();
//                if (!eventType.getSchema().getVersion().equals(recordSchemaVersion)) {
//
//                }

                final EventOwnerSelector eventOwnerSelector = eventType.getEventOwnerSelector();
                if (null == eventOwnerSelector) {
                    continue;
                }

                try {
                    switch (eventOwnerSelector.getType()) {
                        case STATIC:
                            record.setOwner(new EventOwnerHeader(
                                    eventOwnerSelector.getName(),
                                    eventOwnerSelector.getValue()));
                        case PATH:
                            // todo take from metadata (no field at the moment)
                        default:
                            record.setOwner(null);
                    }

                    authValidator.authorizeEventWrite(record);
                } catch (AccessDeniedException e) {
                    return new EventPublishResult(
                            EventPublishingStatus.ABORTED,
                            EventPublishingStep.VALIDATING,
                            prepareResponses(records, record,
                                    EventPublishingStep.VALIDATING, e.explain()));
                }
            }

            return null;
        }
    }

}