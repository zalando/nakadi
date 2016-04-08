package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.BatchItemResponse;
import de.zalando.aruha.nakadi.domain.EventPublishResult;
import de.zalando.aruha.nakadi.domain.EventPublishingStatus;
import de.zalando.aruha.nakadi.domain.EventPublishingStep;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.enrichment.Enrichment;
import de.zalando.aruha.nakadi.exceptions.EnrichmentException;
import de.zalando.aruha.nakadi.exceptions.EventPublishingException;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import de.zalando.aruha.nakadi.partitioning.PartitionResolver;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.utils.TestUtils;
import de.zalando.aruha.nakadi.validation.EventTypeValidator;
import de.zalando.aruha.nakadi.validation.ValidationError;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        verify(topicRepository, times(1)).syncPostBatch(eq(eventType.getName()), any());
    }

    @Test
    public void whenValidationFailsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);

        mockFaultValidation(eventType, event, "error");

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(enrichment, times(0)).enrich(event, eventType);
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
        assertThat(second.getStep(), equalTo(EventPublishingStep.ENRICHING));
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
        mockFaultEnrichment(eventType, event);

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(partitionResolver, times(0)).resolvePartition(eventType, event);
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEnrichmentFailsThenSubsequentItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(2);
        final JSONObject event = batch.getJSONObject(0);

        mockSuccessfulValidation(eventType);
        mockFaultEnrichment(eventType, event);

        final EventPublishResult result = publisher.publish(batch, eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));

        final BatchItemResponse first = result.getResponses().get(0);
        assertThat(first.getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
        assertThat(first.getStep(), equalTo(EventPublishingStep.ENRICHING));
        assertThat(first.getDetail(), equalTo("enrichment error"));

        final BatchItemResponse second = result.getResponses().get(1);
        assertThat(second.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(second.getStep(), equalTo(EventPublishingStep.VALIDATING));
        assertThat(second.getDetail(), is(isEmptyString()));
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

    private void mockFaultEnrichment(final EventType eventType, final JSONObject event) throws EnrichmentException {
        Mockito
                .doThrow(new EnrichmentException("enrichment error"))
                .when(enrichment)
                .enrich(event, eventType);
    }

    private void mockFaultValidation(final EventType eventType, final JSONObject event, final String error) throws Exception {
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
            event.put("foo", TestUtils.randomString());
            events.add(event);
        }

        return new JSONArray(events);
    }
}
