package org.zalando.nakadi.cache;

import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.JsonSchemaValidator;
import org.zalando.nakadi.validation.EventValidatorBuilder;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventTypeCacheTest {
    @Mock
    private ChangesRegistry changesRegistry;
    @Mock
    private EventTypeRepository eventTypeRepository;
    @Mock
    private TimelineDbRepository timelineDbRepository;
    @Mock
    private TopicRepositoryHolder topicRepositoryHolder;
    @Mock
    private TimelineSync timelineSync;
    @Mock
    private EventValidatorBuilder eventValidatorBuilder;
    @Mock
    private SchemaService schemaService;
    @Mock
    private TimelineSync.ListenerRegistration listener;
    private EventTypeCache eventTypeCache;
    @Captor
    private ArgumentCaptor<Runnable> listenerRegistrationCaptor;

    @Before
    public synchronized void setupMocks() {
        MockitoAnnotations.initMocks(this);

        eventTypeCache = new EventTypeCache(
                changesRegistry, eventTypeRepository, timelineDbRepository, topicRepositoryHolder, timelineSync,
                eventValidatorBuilder, schemaService,
                1, 3); // Update every second, so tests should be fast enough
    }

    @Test
    public void ensureThatTimelineSyncListenerIsRegisteredAndDeregistered() {
        when(timelineSync.registerTimelineChangeListener(any())).thenReturn(listener);

        eventTypeCache.startUpdates();
        verify(timelineSync, times(1)).registerTimelineChangeListener(any());

        eventTypeCache.stopUpdates();
        verify(listener, times(1)).cancel();
    }

    @Test(timeout = 10000)
    public synchronized void ensurePeriodicUpdatesAreWorking() {
        final TimelineSync.ListenerRegistration listener = mock(TimelineSync.ListenerRegistration.class);
        when(timelineSync.registerTimelineChangeListener(any())).thenReturn(listener);
        eventTypeCache.startUpdates();

        retry(() -> {
            verify(changesRegistry, times(1)).getCurrentChanges(notNull(Runnable.class));
            verify(changesRegistry, times(2)).getCurrentChanges(isNull(Runnable.class));
        }, 10000);

        eventTypeCache.stopUpdates();
    }

    @Test(timeout = 2000)
    public synchronized void ensureListenerRecreatedOnZkReaction() {
        final TimelineSync.ListenerRegistration listener = mock(TimelineSync.ListenerRegistration.class);
        when(timelineSync.registerTimelineChangeListener(any())).thenReturn(listener);
        eventTypeCache.startUpdates();

        // First registration
        retry(
                () -> verify(changesRegistry, times(1)).getCurrentChanges(listenerRegistrationCaptor.capture()),
                1000);

        // Zookeeper is calling !
        listenerRegistrationCaptor.getValue().run();

        // Check that listener is recreated
        retry(
                () -> verify(changesRegistry, times(2)).getCurrentChanges(notNull(Runnable.class)),
                1000
        );

        eventTypeCache.stopUpdates();
    }

    @Test(timeout = 5000)
    public synchronized void ensureListenerRecreatedOnPeriodicUpdates() throws Exception {
        final TimelineSync.ListenerRegistration listener = mock(TimelineSync.ListenerRegistration.class);
        when(timelineSync.registerTimelineChangeListener(any())).thenReturn(listener);
        eventTypeCache.startUpdates();

        // First registration
        retry(
                () -> verify(changesRegistry, times(1)).getCurrentChanges(notNull(Runnable.class)),
                1000);
        // Not let's say that we have update in zk, but listener was not called
        when(changesRegistry.getCurrentChanges(any()))
                .thenReturn(Collections.singletonList(new Change("test", "testET", new Date())));
        retry(
                () -> verify(changesRegistry, times(2)).getCurrentChanges(notNull(Runnable.class)),
                3000);
        eventTypeCache.stopUpdates();
    }

    @Test(timeout = 6000)
    public synchronized void ensureThatChangesAreDeletedAfterTTL() throws Exception {
        final TimelineSync.ListenerRegistration listener = mock(TimelineSync.ListenerRegistration.class);
        when(timelineSync.registerTimelineChangeListener(any())).thenReturn(listener);
        final Change change = new Change("ch1", "et", new Date());
        when(changesRegistry.getCurrentChanges(any())).thenReturn(Collections.singletonList(change));
        eventTypeCache.startUpdates();
        retry(() -> {
            verify(changesRegistry, atLeastOnce()).deleteChanges(eq(Collections.singletonList(change.getId())));
        }, 5000); // TTL + 2 * periodic check
        eventTypeCache.stopUpdates();
    }

    @Test(timeout = 2000)
    public synchronized void testThatCacheIsActuallyWorkingAndValueIsLoaded() throws Exception {
        final TimelineSync.ListenerRegistration listener = mock(TimelineSync.ListenerRegistration.class);
        when(timelineSync.registerTimelineChangeListener(any())).thenReturn(listener);

        final String eventTypeName = "test";

        final EventType et1 = mock(EventType.class);
        final JsonSchemaValidator validator1 = mock(JsonSchemaValidator.class);

        final Storage storage1 = mock(Storage.class);
        final Timeline timeline1 = Timeline.createTimeline("et1", 0, storage1, "topic1", new Date());
        timeline1.setSwitchedAt(new Date());

        final Storage storage2 = mock(Storage.class);
        final Timeline timeline2 = Timeline.createTimeline("et1", 1, storage2, "topic2", new Date());
        timeline2.setSwitchedAt(new Date());

        final List<Timeline> expectedTimelines1 = List.of(timeline1);
        final List<Timeline> expectedTimelines2 = List.of(timeline1, timeline2);

        final TopicRepository topicRepository1 = mock(TopicRepository.class);
        final TopicRepository topicRepository2 = mock(TopicRepository.class);

        final EventType et2 = mock(EventType.class);
        final JsonSchemaValidator validator2 = mock(JsonSchemaValidator.class);

        final EventTypeSchema etSchema = new EventTypeSchema(new EventTypeSchemaBase(
                EventTypeSchemaBase.Type.JSON_SCHEMA, "{}"),
                "1.0.0", DateTime.now());
        when(et1.getSchema()).thenReturn(etSchema);
        when(et2.getSchema()).thenReturn(etSchema);
        when(eventTypeRepository.findByName(eq(eventTypeName))).thenReturn(et1, et2);
        when(schemaService.getLatestSchemaByType(eventTypeName, EventTypeSchema.Type.JSON_SCHEMA))
                .thenReturn(Optional.of(etSchema));
        when(eventValidatorBuilder.build(eq(et1))).thenReturn(validator1);
        when(eventValidatorBuilder.build(eq(et2))).thenReturn(validator2);
        when(timelineDbRepository.listTimelinesOrdered(eq(eventTypeName)))
                .thenReturn(expectedTimelines1, expectedTimelines2);
        when(topicRepositoryHolder.getTopicRepository(storage1)).thenReturn(topicRepository1);
        when(topicRepositoryHolder.getTopicRepository(storage2)).thenReturn(topicRepository2);
        when(topicRepository1.listPartitionNames("topic1")).thenReturn(List.of("1", "0"));
        when(topicRepository2.listPartitionNames("topic2")).thenReturn(List.of("1", "0", "2"));

        for (int i = 0; i < 10; ++i) { // Verify that cache is still returning the same value without reload
            Assert.assertEquals(et1, eventTypeCache.getEventType(eventTypeName));
            Assert.assertEquals(expectedTimelines1, eventTypeCache.getTimelinesOrdered(eventTypeName));
            Assert.assertEquals(List.of("0", "1"), eventTypeCache.getOrderedPartitions(eventTypeName));
            Assert.assertEquals(validator1, eventTypeCache.getValidator(eventTypeName));
        }

        // Now, let's register new change and start updates.
        when(changesRegistry.getCurrentChanges(notNull(Runnable.class)))
                .thenReturn(Collections.singletonList(new Change("ch1", eventTypeName, new Date())));

        eventTypeCache.startUpdates();
        retry(
                () -> verify(changesRegistry, times(1)).getCurrentChanges(listenerRegistrationCaptor.capture()),
                1000);

        // Zookeeper is calling !
        listenerRegistrationCaptor.getValue().run();

        retry(() -> {
            for (int i = 0; i < 10; ++i) {
                Assert.assertEquals(et2, eventTypeCache.getEventType(eventTypeName));
                Assert.assertEquals(expectedTimelines2, eventTypeCache.getTimelinesOrdered(eventTypeName));
                Assert.assertEquals(List.of("0", "1", "2"), eventTypeCache.getOrderedPartitions(eventTypeName));
                Assert.assertEquals(validator2, eventTypeCache.getValidator(eventTypeName));
            }
        }, 500);
        eventTypeCache.stopUpdates();
    }

    @Test
    public void testThatExceptionFromRepositoryIsPropagated() {
        when(eventTypeRepository.findByName(eq("test"))).thenThrow(new NoSuchEventTypeException("blablabla"));
        try {
            eventTypeCache.getEventType("test");
            Assert.fail();
        } catch (final NoSuchEventTypeException ex) {
            Assert.assertEquals(ex.getMessage(), "blablabla");
        }
    }

    interface RunnableWithException {
        void run() throws Exception;
    }

    private void retry(final RunnableWithException toRun, final long maxWait) {
        executeWithRetry(() -> {
            try {
                toRun.run();
            } catch (final RuntimeException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }, new RetryForSpecifiedTimeStrategy<Void>(maxWait)
                .withExceptionsThatForceRetry(AssertionError.class)
                .withWaitBetweenEachTry(100));
    }

}
