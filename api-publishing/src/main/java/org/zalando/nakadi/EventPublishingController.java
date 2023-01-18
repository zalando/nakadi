package org.zalando.nakadi;

import com.google.common.base.Charsets;
import com.google.common.io.CountingInputStream;
import io.opentracing.tag.Tags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.BlockedException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.kpi.event.NakadiBatchPublished;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.metrics.EventTypeMetrics;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.publishing.BinaryEventPublisher;
import org.zalando.nakadi.service.publishing.EventPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.util.SLOBuckets;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.springframework.http.ResponseEntity.status;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;

@RestController
public class EventPublishingController {

    private final EventPublisher publisher;
    private final BinaryEventPublisher binaryPublisher;
    private final EventTypeMetricRegistry eventTypeMetricRegistry;
    private final BlacklistService blacklistService;
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final NakadiRecordMapper nakadiRecordMapper;
    private final PublishingResultConverter publishingResultConverter;
    private final EventTypeCache eventTypeCache;
    private final AuthorizationValidator authValidator;

    @Autowired
    public EventPublishingController(final EventPublisher publisher,
                                     final BinaryEventPublisher binaryPublisher,
                                     final EventTypeMetricRegistry eventTypeMetricRegistry,
                                     final BlacklistService blacklistService,
                                     final NakadiKpiPublisher nakadiKpiPublisher,
                                     final NakadiRecordMapper nakadiRecordMapper,
                                     final PublishingResultConverter publishingResultConverter,
                                     final EventTypeCache eventTypeCache,
                                     final AuthorizationValidator authValidator) {
        this.publisher = publisher;
        this.binaryPublisher = binaryPublisher;
        this.eventTypeMetricRegistry = eventTypeMetricRegistry;
        this.blacklistService = blacklistService;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.nakadiRecordMapper = nakadiRecordMapper;
        this.publishingResultConverter = publishingResultConverter;
        this.eventTypeCache = eventTypeCache;
        this.authValidator = authValidator;
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/events", method = POST)
    public ResponseEntity postJsonEvents(@PathVariable final String eventTypeName,
                                         @RequestBody final String eventsAsString,
                                         final HttpServletRequest request,
                                         final Client client)
            throws AccessDeniedException, BlockedException, ServiceTemporarilyUnavailableException,
            InternalNakadiException, EventTypeTimeoutException, NoSuchEventTypeException {
        return postEventsWithMetrics(eventTypeName, eventsAsString, request, client, false);
    }

    @RequestMapping(
            value = "/event-types/{eventTypeName}/events",
            method = POST,
            consumes = "application/avro-binary; charset=utf-8",
            produces = "application/json; charset=utf-8"
    )
    public ResponseEntity postBinaryEvents(@PathVariable final String eventTypeName,
                                           final HttpServletRequest request,
                                           final Client client)
            throws AccessDeniedException, BlockedException, ServiceTemporarilyUnavailableException,
            InternalNakadiException, EventTypeTimeoutException, NoSuchEventTypeException {

        // TODO: check that event type schema type is AVRO!

        try {
            return postBinaryEvents(eventTypeName, request.getInputStream(), client, false);
        } catch (IOException e) {
            throw new InternalNakadiException("failed to parse batch", e);
        }
    }

    @RequestMapping(
            value = "/event-types/{eventTypeName}/deleted-events",
            method = POST,
            consumes = "application/avro-binary; charset=utf-8",
            produces = "application/json; charset=utf-8"
    )
    public ResponseEntity deleteBinaryEvents(@PathVariable final String eventTypeName,
                                             final HttpServletRequest request,
                                             final Client client)
            throws AccessDeniedException, BlockedException, ServiceTemporarilyUnavailableException,
            InternalNakadiException, EventTypeTimeoutException, NoSuchEventTypeException {
        try {
            return postBinaryEvents(eventTypeName, request.getInputStream(), client, true);
        } catch (IOException e) {
            throw new InternalNakadiException("failed to parse batch", e);
        }
    }


    private ResponseEntity postBinaryEvents(final String eventTypeName,
                                            final InputStream batch,
                                            final Client client,
                                            final boolean delete) {
        TracingService.setOperationName("publish_events")
                .setTag("event_type", eventTypeName)
                .setTag("сontent-type", "application/avro-binary")
                .setTag(Tags.SPAN_KIND_PRODUCER, client.getClientId());

        if (blacklistService.isProductionBlocked(eventTypeName, client.getClientId())) {
            throw new BlockedException("Application or event type is blocked");
        }

        final EventType eventType = eventTypeCache.getEventType(eventTypeName);

        if (delete && eventType.getCleanupPolicy() == CleanupPolicy.DELETE) {
            throw new InvalidEventTypeException("It is not allowed to delete events from non compacted event type");
        }

        authValidator.authorizeEventTypeWrite(eventType);

        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);
        try {
            final long startingNanos = System.nanoTime();
            try {
                final CountingInputStream countingInputStream = new CountingInputStream(batch);
                final List<NakadiRecord> nakadiRecords = nakadiRecordMapper.fromBytesBatch(countingInputStream);
                final List<NakadiRecordResult> recordResults;
                if (delete) {
                    recordResults = binaryPublisher.delete(nakadiRecords, eventType);
                } else {
                    recordResults = binaryPublisher.publish(eventType, nakadiRecords);
                }
                if (recordResults.isEmpty()) {
                    throw new InternalNakadiException("unexpected empty record result list, " +
                            "publishing record result can not be empty");
                }
                final EventPublishResult result = publishingResultConverter.mapPublishingResultToView(recordResults);
                final int eventCount = result.getResponses().size();

                final long totalSizeBytes = countingInputStream.getCount();
                TracingService.setTag("slo_bucket", SLOBuckets.getNameForBatchSize(totalSizeBytes));

                reportMetrics(eventTypeMetrics, result, totalSizeBytes, eventCount);
                reportSLOs(startingNanos, totalSizeBytes, eventCount, result, eventTypeName, client);

                if (result.getStatus() == EventPublishingStatus.FAILED) {
                    TracingService.setErrorFlag();
                }

                final ResponseEntity response = response(result);
                eventTypeMetrics.incrementResponseCount(response.getStatusCode().value());
                return response;
            } finally {
                eventTypeMetrics.updateTiming(startingNanos, System.nanoTime());
            }
        } catch (final NoSuchEventTypeException exception) {
            eventTypeMetrics.incrementResponseCount(NOT_FOUND.getStatusCode());
            throw exception;
        } catch (final RuntimeException ex) {
            eventTypeMetrics.incrementResponseCount(INTERNAL_SERVER_ERROR.getStatusCode());
            throw ex;
        }
    }

    @RequestMapping(value = "/event-types/{eventTypeName}/deleted-events", method = POST)
    public ResponseEntity deleteEvents(@PathVariable final String eventTypeName,
                                       @RequestBody final String eventsAsString,
                                       final HttpServletRequest request,
                                       final Client client) {
        return postEventsWithMetrics(eventTypeName, eventsAsString, request, client, true);
    }

    private ResponseEntity postEventsWithMetrics(final String eventTypeName,
                                                 final String eventsAsString,
                                                 final HttpServletRequest request,
                                                 final Client client,
                                                 final boolean delete) {
        TracingService.setOperationName("publish_events")
                .setTag("event_type", eventTypeName)
                .setTag(Tags.SPAN_KIND_PRODUCER, client.getClientId());

        if (blacklistService.isProductionBlocked(eventTypeName, client.getClientId())) {
            throw new BlockedException("Application or event type is blocked");
        }
        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(eventTypeName);
        try {
            final ResponseEntity response = postEventInternal(
                    eventTypeName, eventsAsString, eventTypeMetrics, client, request, delete);
            eventTypeMetrics.incrementResponseCount(response.getStatusCode().value());
            return response;
        } catch (final NoSuchEventTypeException exception) {
            eventTypeMetrics.incrementResponseCount(NOT_FOUND.getStatusCode());
            throw exception;
        } catch (final RuntimeException ex) {
            eventTypeMetrics.incrementResponseCount(INTERNAL_SERVER_ERROR.getStatusCode());
            throw ex;
        }
    }

    private ResponseEntity postEventInternal(final String eventTypeName,
                                             final String eventsAsString,
                                             final EventTypeMetrics eventTypeMetrics,
                                             final Client client,
                                             final HttpServletRequest request,
                                             final boolean delete)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException, InternalNakadiException,
            EventTypeTimeoutException, NoSuchEventTypeException {
        final long startingNanos = System.nanoTime();
        try {
            final EventPublishResult result;

            final int totalSizeBytes = eventsAsString.getBytes(Charsets.UTF_8).length;
            TracingService.setTag("slo_bucket", SLOBuckets.getNameForBatchSize(totalSizeBytes));

            if (delete) {
                result = publisher.delete(eventsAsString, eventTypeName);
            } else {
                result = publisher.publish(eventsAsString, eventTypeName);
            }
            final int eventCount = result.getResponses().size();

            reportMetrics(eventTypeMetrics, result, totalSizeBytes, eventCount);
            reportSLOs(startingNanos, totalSizeBytes, eventCount, result, eventTypeName, client);

            if (result.getStatus() == EventPublishingStatus.FAILED) {
                TracingService.setErrorFlag();
            }

            return response(result);
        } finally {
            eventTypeMetrics.updateTiming(startingNanos, System.nanoTime());
        }
    }

    private void reportSLOs(final long startingNanos, final long totalSizeBytes, final int eventCount,
                            final EventPublishResult eventPublishResult, final String eventTypeName,
                            final Client client) {
        if (eventPublishResult.getStatus() == EventPublishingStatus.SUBMITTED) {
            final long msSpent = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startingNanos);
            final String applicationName = client.getClientId();

            nakadiKpiPublisher.publish(() -> NakadiBatchPublished.newBuilder()
                    .setEventType(eventTypeName)
                    .setApp(applicationName)
                    .setAppHashed(nakadiKpiPublisher.hash(applicationName))
                    .setTokenRealm(client.getRealm())
                    .setNumberOfEvents(eventCount)
                    .setMsSpent(msSpent)
                    .setBatchSize(totalSizeBytes)
                    .build());
        }
    }

    private void reportMetrics(final EventTypeMetrics eventTypeMetrics, final EventPublishResult result,
                               final long totalSizeBytes, final int eventCount) {
        if (result.getStatus() == EventPublishingStatus.SUBMITTED) {
            eventTypeMetrics.reportSizing(eventCount, totalSizeBytes - eventCount - 1);
        } else if (result.getStatus() == EventPublishingStatus.FAILED && eventCount != 0) {
            final int successfulEvents = result.getResponses()
                    .stream()
                    .filter(r -> r.getPublishingStatus() == EventPublishingStatus.SUBMITTED)
                    .collect(Collectors.toList())
                    .size();
            final double avgEventSize = totalSizeBytes / (double) eventCount;
            eventTypeMetrics.reportSizing(successfulEvents, (int) Math.round(avgEventSize * successfulEvents));
        }
    }

    private ResponseEntity response(final EventPublishResult result) {
        switch (result.getStatus()) {
            case SUBMITTED:
                return status(HttpStatus.OK).build();
            case ABORTED:
                return status(HttpStatus.UNPROCESSABLE_ENTITY).body(result.getResponses());
            default:
                return status(HttpStatus.MULTI_STATUS).body(result.getResponses());
        }
    }
}
