package org.zalando.nakadi.service;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.EnrichmentException;
import org.zalando.nakadi.exceptions.EventPublishingException;
import org.zalando.nakadi.exceptions.PartitioningException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.ValidationError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.zalando.nakadi.utils.TestUtils.buildBusinessEvent;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.createBatch;
import static org.zalando.nakadi.utils.TestUtils.randomString;

public class EventPublisherTest {

    private final TopicRepository topicRepository = mock(TopicRepository.class);
    private final EventTypeCache cache = mock(EventTypeCache.class);
    private final PartitionResolver partitionResolver = mock(PartitionResolver.class);
    private final Enrichment enrichment = mock(Enrichment.class);
    private final EventPublisher publisher = new EventPublisher(topicRepository, cache, partitionResolver, enrichment);

    @Test
    public void whenPublishIsSuccessfulThenResultIsSubmitted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);

        mockSuccessfulValidation(eventType, event);

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.SUBMITTED));
        verify(topicRepository, times(1)).syncPostBatch(eq(eventType.getTopic()), any());
    }

    @Test
    public void whenEventHasEidThenSetItInTheResponse() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final JSONArray batch = new JSONArray(Arrays.asList(event));

        mockSuccessfulValidation(eventType, event);

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getResponses().get(0).getEid(), equalTo(event.getJSONObject("metadata").optString("eid")));
        verify(topicRepository, times(1)).syncPostBatch(eq(eventType.getTopic()), any());
    }

    @Test
    public void whenValidationFailsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);

        mockFaultValidation(eventType, event, "error");

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(enrichment, times(0)).enrich(createBatch(event), eventType);
        verify(partitionResolver, times(0)).resolvePartition(eventType, event);
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenValidationFailsThenSubsequentItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(2);
        final JSONObject event = batch.getJSONObject(0);

        mockFaultValidation(eventType, event, "error");

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));

        final BatchItemResponse first = result.getResponses().get(0);
        assertThat(first.getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
        assertThat(first.getStep(), equalTo(EventPublishingStep.VALIDATING));
        assertThat(first.getDetail(), equalTo("error"));

        final BatchItemResponse second = result.getResponses().get(1);
        assertThat(second.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(second.getStep(), equalTo(EventPublishingStep.NONE));
        assertThat(second.getDetail(), is(isEmptyString()));

        verify(cache, times(1)).getValidator(any());
    }

    @Test
    public void whenPartitionFailsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);

        mockSuccessfulValidation(eventType, event);
        mockFaultPartition(eventType, event);

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
    }

    @Test
    public void whenPartitionFailsThenSubsequentItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(2);
        final JSONObject event = batch.getJSONObject(0);

        mockSuccessfulValidation(eventType);
        mockFaultPartition(eventType, event);

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));

        final BatchItemResponse first = result.getResponses().get(0);
        assertThat(first.getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
        assertThat(first.getStep(), equalTo(EventPublishingStep.PARTITIONING));
        assertThat(first.getDetail(), equalTo("partition error"));

        final BatchItemResponse second = result.getResponses().get(1);
        assertThat(second.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(second.getStep(), equalTo(EventPublishingStep.VALIDATING));
        assertThat(second.getDetail(), is(isEmptyString()));

        verify(cache, times(2)).getValidator(any());
        verify(partitionResolver, times(1)).resolvePartition(any(), any());
    }

    @Test
    public void whenPublishingFailsThenResultIsFailed() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);

        mockSuccessfulValidation(eventType);
        mockFailedPublishing();

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.FAILED));
        verify(topicRepository, times(1)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEnrichmentFailsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);

        mockSuccessfulValidation(eventType, event);
        mockFaultEnrichment();

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(cache, times(1)).getValidator(eventType.getName());
        verify(partitionResolver, times(1)).resolvePartition(eventType, event);
        verify(enrichment, times(1)).enrich(any(), any());
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEnrichmentFailsThenSubsequentItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(2);

        mockSuccessfulValidation(eventType);
        mockFaultEnrichment();

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));

        final BatchItemResponse first = result.getResponses().get(0);
        assertThat(first.getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
        assertThat(first.getStep(), equalTo(EventPublishingStep.ENRICHING));
        assertThat(first.getDetail(), equalTo("enrichment error"));

        final BatchItemResponse second = result.getResponses().get(1);
        assertThat(second.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(second.getStep(), equalTo(EventPublishingStep.PARTITIONING));
        assertThat(second.getDetail(), is(isEmptyString()));

        verify(enrichment, times(1)).enrich(any(), any());
    }

    private void mockFailedPublishing() throws Exception {
        Mockito
                .doThrow(EventPublishingException.class)
                .when(topicRepository)
                .syncPostBatch(any(), any());
    }

    private void mockFaultPartition(final EventType eventType, final JSONObject event) throws PartitioningException {
        Mockito
                .doThrow(new PartitioningException("partition error"))
                .when(partitionResolver)
                .resolvePartition(eventType, event);
    }

    private void mockFaultEnrichment() throws EnrichmentException {
        Mockito
                .doThrow(new EnrichmentException("enrichment error"))
                .when(enrichment)
                .enrich(any(), any());
    }

    private void mockFaultValidation(final EventType eventType, final JSONObject event, final String error)
            throws Exception {
        final EventTypeValidator faultyValidator = mock(EventTypeValidator.class);

        Mockito
                .doReturn(eventType)
                .when(cache)
                .getEventType(eventType.getName());

        Mockito
                .doReturn(faultyValidator)
                .when(cache)
                .getValidator(eventType.getName());

        Mockito
                .doReturn(Optional.of(new ValidationError(error)))
                .when(faultyValidator)
                .validate(event);
    }

    private void mockSuccessfulValidation(final EventType eventType, final JSONObject event) throws Exception {
        final EventTypeValidator truthyValidator = mock(EventTypeValidator.class);

        Mockito
                .doReturn(eventType)
                .when(cache)
                .getEventType(eventType.getName());

        Mockito
                .doReturn(Optional.empty())
                .when(truthyValidator)
                .validate(event);

        Mockito
                .doReturn(truthyValidator)
                .when(cache)
                .getValidator(eventType.getName());
    }

    private void mockSuccessfulValidation(final EventType eventType) throws Exception {
        final EventTypeValidator truthyValidator = mock(EventTypeValidator.class);

        Mockito
                .doReturn(eventType)
                .when(cache)
                .getEventType(eventType.getName());

        Mockito
                .doReturn(Optional.empty())
                .when(truthyValidator)
                .validate(any());

        Mockito
                .doReturn(truthyValidator)
                .when(cache)
                .getValidator(eventType.getName());
    }

    private JSONArray buildDefaultBatch(final int numberOfEvents) {
        final List<JSONObject> events = new ArrayList<>();

        for (int i = 0; i < numberOfEvents; i++) {
            final JSONObject event = new JSONObject();
            event.put("foo", randomString());
            events.add(event);
        }

        return new JSONArray(events);
    }

}
