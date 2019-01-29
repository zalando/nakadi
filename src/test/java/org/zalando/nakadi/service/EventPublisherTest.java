package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.EventPublishingException;
import org.zalando.nakadi.exceptions.runtime.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.ValidationError;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.zalando.nakadi.utils.TestUtils.buildBusinessEvent;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.createBatchItem;
import static org.zalando.nakadi.utils.TestUtils.randomString;
import static org.zalando.nakadi.utils.TestUtils.randomStringOfLength;
import static org.zalando.nakadi.utils.TestUtils.randomValidStringOfLength;

public class EventPublisherTest {

    private static final int NAKADI_SEND_TIMEOUT = 10000;
    private static final int NAKADI_POLL_TIMEOUT = 10000;
    private static final int NAKADI_EVENT_MAX_BYTES = 900;
    private static final long TOPIC_RETENTION_TIME_MS = 150;
    private static final long TIMELINE_WAIT_TIMEOUT_MS = 1000;
    private static final int NAKADI_SUBSCRIPTION_MAX_PARTITIONS = 8;

    private final TopicRepository topicRepository = mock(TopicRepository.class);
    private final EventTypeCache cache = mock(EventTypeCache.class);
    private final PartitionResolver partitionResolver = mock(PartitionResolver.class);
    private final TimelineSync timelineSync = mock(TimelineSync.class);
    private final Enrichment enrichment = mock(Enrichment.class);
    private final AuthorizationValidator authzValidator = mock(AuthorizationValidator.class);
    private final NakadiSettings nakadiSettings = new NakadiSettings(0, 0, 0, TOPIC_RETENTION_TIME_MS, 0, 60,
            NAKADI_POLL_TIMEOUT, NAKADI_SEND_TIMEOUT, TIMELINE_WAIT_TIMEOUT_MS, NAKADI_EVENT_MAX_BYTES,
            NAKADI_SUBSCRIPTION_MAX_PARTITIONS, "service", "nakadi", "", "", 10);
    private final EventPublisher publisher;

    public EventPublisherTest() {
        final TimelineService ts = Mockito.mock(TimelineService.class);
        Mockito.when(ts.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        Mockito.when(ts.getTopicRepository((EventTypeBase) any())).thenReturn(topicRepository);
        final Timeline timeline = Mockito.mock(Timeline.class);
        Mockito.when(ts.getActiveTimeline(any(EventType.class))).thenReturn(timeline);

        publisher = new EventPublisher(ts, cache, partitionResolver, enrichment, nakadiSettings, timelineSync,
                authzValidator);
    }

    @Test
    public void whenPublishIsSuccessfulThenResultIsSubmitted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.SUBMITTED));
        verify(topicRepository, times(1)).syncPostBatch(any(), any());
    }

    @Test(expected = AccessDeniedException.class)
    public void whenPublishAuthorizationIsTakenIntoAccount() throws Exception {
        final EventType et = buildDefaultEventType();

        mockSuccessfulValidation(et);

        Mockito.doThrow(new AccessDeniedException(null, null))
                .when(authzValidator)
                .authorizeEventTypeWrite(Mockito.eq(et));

        publisher.publish(buildDefaultBatch(1).toString(), et.getName());
    }

    @Test
    public void whenEventHasEidThenSetItInTheResponse() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONObject event = buildBusinessEvent();
        final JSONArray batch = new JSONArray(Arrays.asList(event));

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getResponses().get(0).getEid(), equalTo(event.getJSONObject("metadata").optString("eid")));
        verify(topicRepository, times(1)).syncPostBatch(any(), any());
    }

    @Test
    public void whenPublishEventTypeIsLockedAndReleased() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        mockSuccessfulValidation(eventType);

        final Closeable etCloser = mock(Closeable.class);
        Mockito.when(timelineSync.workWithEventType(any(String.class), anyLong())).thenReturn(etCloser);

        publisher.publish(batch.toString(), eventType.getName());

        verify(timelineSync, times(1)).workWithEventType(eq(eventType.getName()), eq(TIMELINE_WAIT_TIMEOUT_MS));
        verify(etCloser, times(1)).close();
    }

    @Test(expected = EventTypeTimeoutException.class)
    public void whenPublishAndTimelineLockTimedOutThenException() throws Exception {
        Mockito.when(timelineSync.workWithEventType(any(String.class), anyLong())).thenThrow(new TimeoutException());
        publisher.publish(buildDefaultBatch(0).toString(), "blahET");
    }

    @Test
    public void whenValidationFailsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);

        mockFaultValidation(eventType, "error");

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(enrichment, times(0)).enrich(createBatchItem(event), eventType);
        verify(partitionResolver, times(0)).resolvePartition(eventType, event);
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenValidationFailsThenSubsequentItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(2);
        final JSONObject event = batch.getJSONObject(0);

        mockFaultValidation(eventType, "error");

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

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
    public void whenEventIsTooLargeThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildLargeBatch(1);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(enrichment, times(0)).enrich(any(), any());
        verify(partitionResolver, times(0)).resolvePartition(any(), any());
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEventIsTooLargeThenAllItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject largeEvent = new JSONObject();
        largeEvent.put("foo", randomStringOfLength(10000));
        batch.put(largeEvent);
        final JSONObject smallEvent = new JSONObject();
        smallEvent.put("foo", randomString());
        batch.put(smallEvent);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));

        final BatchItemResponse firstResponse = result.getResponses().get(0);
        assertThat(firstResponse.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(firstResponse.getStep(), equalTo(EventPublishingStep.VALIDATING));
        assertThat(firstResponse.getDetail(), is(isEmptyString()));

        final BatchItemResponse secondResponse = result.getResponses().get(1);
        assertThat(secondResponse.getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
        assertThat(secondResponse.getStep(), equalTo(EventPublishingStep.VALIDATING));
        assertThat(secondResponse.getDetail(), startsWith("Event too large"));

        final BatchItemResponse thirdResponse = result.getResponses().get(2);
        assertThat(thirdResponse.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(thirdResponse.getStep(), equalTo(EventPublishingStep.NONE));
        assertThat(thirdResponse.getDetail(), is(isEmptyString()));
    }

    @Test
    public void whenEnrichmentMakesEventTooLargeThenAllItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject largeEvent = new JSONObject();
        largeEvent.put("foo", randomStringOfLength(880));
        batch.put(largeEvent);
        final JSONObject smallEvent = new JSONObject();
        smallEvent.put("foo", randomString());
        batch.put(smallEvent);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));

        final BatchItemResponse firstResponse = result.getResponses().get(0);
        assertThat(firstResponse.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(firstResponse.getStep(), equalTo(EventPublishingStep.VALIDATING));
        assertThat(firstResponse.getDetail(), is(isEmptyString()));

        final BatchItemResponse secondResponse = result.getResponses().get(1);
        assertThat(secondResponse.getPublishingStatus(), equalTo(EventPublishingStatus.FAILED));
        assertThat(secondResponse.getStep(), equalTo(EventPublishingStep.VALIDATING));
        assertThat(secondResponse.getDetail(), startsWith("Event too large"));

        final BatchItemResponse thirdResponse = result.getResponses().get(2);
        assertThat(thirdResponse.getPublishingStatus(), equalTo(EventPublishingStatus.ABORTED));
        assertThat(thirdResponse.getStep(), equalTo(EventPublishingStep.NONE));
        assertThat(thirdResponse.getDetail(), is(isEmptyString()));
    }

    @Test
    public void whenEventIsExactlyMaxSizeThenResultIsSuccess() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildMaxSizeBatch(1);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.SUBMITTED));
        verify(enrichment, times(1)).enrich(any(), any());
        verify(partitionResolver, times(1)).resolvePartition(any(), any());
        verify(topicRepository, times(1)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEventIsOneByteOverMaxSizeThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildOneByteTooLargeBatch(1);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(enrichment, times(0)).enrich(any(), any());
        verify(partitionResolver, times(0)).resolvePartition(any(), any());
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEventIsOneByteOverMaxSizeWithMultiByteCharsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildOneByteTooLargeBatchMultiByte(1);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(enrichment, times(0)).enrich(any(), any());
        verify(partitionResolver, times(0)).resolvePartition(any(), any());
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEventIsExactlyMaxSizeWithMultiByteCharsThenResultIsSuccess() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildMaxSizeBatchMultiByte(1);

        mockSuccessfulValidation(eventType);

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.SUBMITTED));
        verify(enrichment, times(1)).enrich(any(), any());
        verify(partitionResolver, times(1)).resolvePartition(any(), any());
        verify(topicRepository, times(1)).syncPostBatch(any(), any());
    }

    @Test
    public void whenPartitionFailsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(createBatchItem(buildDefaultBatch(1).getJSONObject(0)));
        final JSONObject event = batch.get(0).getEvent();

        mockSuccessfulValidation(eventType);
        mockFaultPartition();

        final EventPublishResult result = publisher.publish(createStringFromBatchItems(batch), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
    }

    @Test
    public void whenPartitionFailsThenSubsequentItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray array = buildDefaultBatch(2);
        final List<BatchItem> batch = new ArrayList<>();
        batch.add(createBatchItem(array.getJSONObject(0)));
        batch.add(createBatchItem(array.getJSONObject(1)));

        mockSuccessfulValidation(eventType);
        mockFaultPartition();

        final EventPublishResult result = publisher.publish(createStringFromBatchItems(batch), eventType.getName());

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

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.FAILED));
        verify(topicRepository, times(1)).syncPostBatch(any(), any());
    }

    @Test
    public void whenEnrichmentFailsThenResultIsAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);

        mockSuccessfulValidation(eventType);
        mockFaultEnrichment();

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

        assertThat(result.getStatus(), equalTo(EventPublishingStatus.ABORTED));
        verify(cache, times(1)).getValidator(eventType.getName());
        verify(partitionResolver, times(1)).resolvePartition(any(), any());
        verify(enrichment, times(1)).enrich(any(), any());
        verify(topicRepository, times(0)).syncPostBatch(any(), any());
    }

    @Test
    public void whenSinglePartitioningKeyThenEventKeyIsSet() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.HASH_STRATEGY)
                .partitionKeyFields(ImmutableList.of("my_field"))
                .build();

        final JSONArray batch = buildDefaultBatch(1);
        batch.getJSONObject(0).put("my_field", "my_key");

        mockSuccessfulValidation(eventType);

        publisher.publish(batch.toString(), eventType.getName());

        final List<BatchItem> publishedBatch = capturePublishedBatch();
        assertThat(publishedBatch.get(0).getEventKey(), equalTo("my_key"));
    }

    @Test
    public void whenMultiplePartitioningKeyThenEventKeyIsNotSet() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.HASH_STRATEGY)
                .partitionKeyFields(ImmutableList.of("my_field", "other_field"))
                .build();

        final JSONArray batch = buildDefaultBatch(1);
        final JSONObject event = batch.getJSONObject(0);
        event.put("my_field", "my_key");
        event.put("other_field", "other_value");

        mockSuccessfulValidation(eventType);

        publisher.publish(batch.toString(), eventType.getName());

        final List<BatchItem> publishedBatch = capturePublishedBatch();
        assertThat(publishedBatch.get(0).getEventKey(), equalTo(null));
    }

    @Test
    public void whenNoneHashPartitioningStrategyThenEventKeyIsNotSet() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.RANDOM_STRATEGY)
                .build();
        final JSONArray batch = buildDefaultBatch(1);

        mockSuccessfulValidation(eventType);

        publisher.publish(batch.toString(), eventType.getName());

        final List<BatchItem> publishedBatch = capturePublishedBatch();
        assertThat(publishedBatch.get(0).getEventKey(), equalTo(null));
    }

    @SuppressWarnings("unchecked")
    private List<BatchItem> capturePublishedBatch() {
        final ArgumentCaptor<List> batchCaptor = ArgumentCaptor.forClass(List.class);
        verify(topicRepository, atLeastOnce()).syncPostBatch(any(), batchCaptor.capture());
        return (List<BatchItem>) batchCaptor.getValue();
    }

    @Test
    public void whenEnrichmentFailsThenSubsequentItemsAreAborted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final JSONArray batch = buildDefaultBatch(2);

        mockSuccessfulValidation(eventType);
        mockFaultEnrichment();

        final EventPublishResult result = publisher.publish(batch.toString(), eventType.getName());

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

    @Test
    public void testWrite() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        Mockito.when(cache.getEventType(eventType.getName())).thenReturn(eventType);
        mockSuccessfulValidation(eventType);
        final EventPublishResult result = publisher.publish(buildDefaultBatch(0).toString(), eventType.getName());

        Assert.assertEquals(result.getStatus(), EventPublishingStatus.SUBMITTED);
    }

    private void mockFailedPublishing() throws Exception {
        Mockito
                .doThrow(EventPublishingException.class)
                .when(topicRepository)
                .syncPostBatch(any(), any());
    }

    private void mockFaultPartition() throws PartitioningException {
        Mockito
                .doThrow(new PartitioningException("partition error"))
                .when(partitionResolver)
                .resolvePartition(any(), any());
    }

    private void mockFaultEnrichment() throws EnrichmentException {
        Mockito
                .doThrow(new EnrichmentException("enrichment error"))
                .when(enrichment)
                .enrich(any(), any());
    }

    private void mockFaultValidation(final EventType eventType, final String error) throws Exception {
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
                .validate(any());
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
        return buildBatch(numberOfEvents, 50);
    }

    private JSONArray buildLargeBatch(final int numberOfEvents) {
        return buildBatch(numberOfEvents, NAKADI_EVENT_MAX_BYTES + 100);
    }

    private JSONArray buildMaxSizeBatch(final int numberOfEvents) {
        return buildBatch(numberOfEvents, NAKADI_EVENT_MAX_BYTES);
    }

    private JSONArray buildOneByteTooLargeBatch(final int numberOfEvents) {
        return buildBatch(numberOfEvents, NAKADI_EVENT_MAX_BYTES + 1);
    }

    private JSONArray buildMaxSizeBatchMultiByte(final int numberOfEvents) {
        return buildBatchMultiByte(numberOfEvents, NAKADI_EVENT_MAX_BYTES);
    }

    private JSONArray buildOneByteTooLargeBatchMultiByte(final int numberOfEvents) {
        return buildBatchMultiByte(numberOfEvents, NAKADI_EVENT_MAX_BYTES + 1);
    }

    private JSONArray buildBatchMultiByte(final int numberOfEvents, final int length) {
        final List<JSONObject> events = new ArrayList<>();
        final int valueLength = length - 16; // each character 2 lines below is 3 bytes
        for (int i = 0; i < numberOfEvents; i++) {
            final JSONObject event = new JSONObject();
            event.put("foo", randomValidStringOfLength(valueLength) + "温泉");
            events.add(event);
        }

        return new JSONArray(events);
    }

    private JSONArray buildBatch(final int numberOfEvents, final int length) {
        final List<JSONObject> events = new ArrayList<>();
        final int valueLength = length - 10; // the brackets, key, and quotation marks take 10 characters
        for (int i = 0; i < numberOfEvents; i++) {
            final JSONObject event = new JSONObject();
            event.put("foo", randomValidStringOfLength(valueLength));
            events.add(event);
        }

        return new JSONArray(events);
    }

    private String createStringFromBatchItems(final List<BatchItem> batch) {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (final BatchItem item : batch) {
            sb.append(item.getEvent().toString());
            sb.append(",");
        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }
}
