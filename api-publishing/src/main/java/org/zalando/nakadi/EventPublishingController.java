package org.zalando.nakadi;

import com.google.common.base.Charsets;
import com.google.common.io.CountingInputStream;
import io.opentracing.tag.Tags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.HeaderTag;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.BlockedException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidConsumerTagException;
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

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    private static final String X_CONSUMER_TAG = "X-CONSUMER-TAG";

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
                                         final Client client,
                                         final @RequestHeader(value = "X-TIMEOUT", required = false, defaultValue = "0")
                                             int publishTimeout)
            throws AccessDeniedException, BlockedException, ServiceTemporarilyUnavailableException,
            InternalNakadiException, EventTypeTimeoutException, NoSuchEventTypeException {
        return postEventsWithMetrics(PublishRequest.asPublish(eventTypeName, eventsAsString,
                client, toHeaderTagMap(request.getHeader(X_CONSUMER_TAG)), publishTimeout));
    }

    @RequestMapping(
            value = "/event-types/{eventTypeName}/events",
            method = POST,
            consumes = "application/avro-binary; charset=utf-8",
            produces = "application/json; charset=utf-8"
    )
    public ResponseEntity postBinaryEvents(@PathVariable final String eventTypeName,
                                           final HttpServletRequest request,
                                           final Client client,
                                           final @RequestHeader(value = "X-TIMEOUT",
                                                   required = false, defaultValue = "0")
                                               int publishTimeout)
            throws AccessDeniedException, BlockedException, ServiceTemporarilyUnavailableException,
            InternalNakadiException, EventTypeTimeoutException, NoSuchEventTypeException {

        // TODO: check that event type schema type is AVRO!

        try {
            return postBinaryEvents(PublishRequest.asPublish(eventTypeName, request.getInputStream(),
                    client, toHeaderTagMap(request.getHeader(X_CONSUMER_TAG)), publishTimeout));
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
            return postBinaryEvents(PublishRequest.asDelete(eventTypeName, request.getInputStream(),
                    client));
        } catch (IOException e) {
            throw new InternalNakadiException("failed to parse batch", e);
        }
    }


    private ResponseEntity postBinaryEvents(final PublishRequest<? extends InputStream> request) {
        TracingService.setOperationName("publish_events")
                .setTag("event_type", request.getEventTypeName())
                .setTag("сontent-type", "application/avro-binary")
                .setTag(Tags.SPAN_KIND_PRODUCER, request.getClient().getClientId());

        if (blacklistService.isProductionBlocked(request.getEventTypeName(),
                request.getClient().getClientId())) {
            throw new BlockedException("Application or event type is blocked");
        }

        final EventType eventType = eventTypeCache.getEventType(request.getEventTypeName());

        if (request.isDeleteRequest() &&
                eventType.getCleanupPolicy() == CleanupPolicy.DELETE) {
            throw new InvalidEventTypeException("It is not allowed to delete events from non compacted event type");
        }

        authValidator.authorizeEventTypeWrite(eventType);

        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(request.getEventTypeName());
        try {
            final long startingNanos = System.nanoTime();
            try {
                final CountingInputStream countingInputStream = new CountingInputStream(request.getEventsRaw());
                final List<NakadiRecord> nakadiRecords = nakadiRecordMapper.fromBytesBatch(countingInputStream);
                final List<NakadiRecordResult> recordResults;
                if (request.isDeleteRequest()) {
                    recordResults = binaryPublisher.delete(nakadiRecords, eventType);
                } else {
                    recordResults = binaryPublisher.publish(eventType, nakadiRecords,
                            request.getConsumerTags(), request.getDesiredPublishingTimeout());
                }
                if (recordResults.isEmpty()) {
                    throw new InternalNakadiException("unexpected empty record result list, " +
                            "publishing record result can not be empty");
                }
                final EventPublishResult result = publishingResultConverter.mapPublishingResultToView(recordResults);

                final int eventCount = nakadiRecords.size();
                TracingService.setTag("number_of_events", eventCount);

                final long totalSizeBytes = countingInputStream.getCount();
                TracingService.setTag("slo_bucket", TracingService.getSLOBucketName(totalSizeBytes));

                reportMetrics(eventTypeMetrics, result, totalSizeBytes, eventCount);
                reportSLOs(startingNanos, totalSizeBytes, eventCount, result,
                        request.getEventTypeName(), request.getClient());

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
        return postEventsWithMetrics(PublishRequest.asDelete(eventTypeName, eventsAsString,
                client));
    }

    private ResponseEntity postEventsWithMetrics(final PublishRequest<String> request) {
        TracingService.setOperationName("publish_events")
                .setTag("event_type", request.getEventTypeName())
                .setTag(Tags.SPAN_KIND_PRODUCER, request.getClient().getClientId());

        if (blacklistService.
                isProductionBlocked(request.getEventTypeName(), request.getClient().getClientId())) {
            throw new BlockedException("Application or event type is blocked");
        }
        final EventTypeMetrics eventTypeMetrics = eventTypeMetricRegistry.metricsFor(request.getEventTypeName());
        try {
            final ResponseEntity response = postEventInternal(request, eventTypeMetrics);
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

    private ResponseEntity postEventInternal(final PublishRequest<String> request,
                                             final EventTypeMetrics eventTypeMetrics)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException, InternalNakadiException,
            EventTypeTimeoutException, NoSuchEventTypeException {
        final long startingNanos = System.nanoTime();
        try {
            final EventPublishResult result;

            final int totalSizeBytes = request.getEventsRaw().getBytes(Charsets.UTF_8).length;
            TracingService.setTag("slo_bucket", TracingService.getSLOBucketName(totalSizeBytes));

            if (request.isDeleteRequest()) {
                result = publisher.delete(request.getEventsRaw(), request.getEventTypeName());
            } else {
                result = publisher.publish(request.getEventsRaw(), request.getEventTypeName(),
                        request.getConsumerTags(), request.getDesiredPublishingTimeout());
            }
            // FIXME: there should be a more direct way to get the input batch size
            final int eventCount = result.getResponses().size();

            reportMetrics(eventTypeMetrics, result, totalSizeBytes, eventCount);
            reportSLOs(startingNanos, totalSizeBytes, eventCount, result,
                    request.getEventTypeName(), request.getClient());

            TracingService.setTag("number_of_events", eventCount);

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

    public static Map<HeaderTag, String> toHeaderTagMap(final String consumerString) {
        if (consumerString == null) {
            return Collections.emptyMap();
        }

        if (consumerString.isBlank()) {
            throw new InvalidConsumerTagException(X_CONSUMER_TAG + ": is empty!");
        }

        final var arr = consumerString.split(",");
        final Map<HeaderTag, String> result = new HashMap<>();
        for (final String entry : arr) {
            final var tagAndValue = entry.replaceAll("\\s", "").split("=");
            if (tagAndValue.length != 2) {
                throw new InvalidConsumerTagException("header tag parameter is imbalanced, " +
                        "expected: 2 but provided " + arr.length);
            }

            final var optHeaderTag = HeaderTag.fromString(tagAndValue[0]);
            if (optHeaderTag.isEmpty()) {
                throw new InvalidConsumerTagException("invalid header tag: " + tagAndValue[0]);
            }
            if (result.containsKey(optHeaderTag.get())) {
                throw new InvalidConsumerTagException("duplicate header tag: "
                        + optHeaderTag.get());
            }
            if (optHeaderTag.get() == HeaderTag.CONSUMER_SUBSCRIPTION_ID) {
                try {
                    UUID.fromString(tagAndValue[1]);
                } catch (IllegalArgumentException e) {
                    throw new InvalidConsumerTagException("header tag value: " + tagAndValue[1] + " is not an UUID");
                }
            }
            result.put(optHeaderTag.get(), tagAndValue[1]);
        }
        return result;
    }
}
