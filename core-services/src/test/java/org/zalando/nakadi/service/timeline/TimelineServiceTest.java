package org.zalando.nakadi.service.timeline;

import com.google.common.collect.ImmutableList;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TimelinesNotSupportedException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.TestSchemaProviderService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TimelineServiceTest {

    @Mock
    private EventTypeCache eventTypeCache;
    @Mock
    private StorageDbRepository storageDbRepository;
    @Mock
    private AdminService adminService;
    @Mock
    private TimelineDbRepository timelineDbRepository;
    @Mock
    private TopicRepositoryHolder topicRepositoryHolder;
    @Mock
    private FeatureToggleService featureToggleService;
    @Mock
    private LocalSchemaRegistry localSchemaRegistry;
    private TimelineService timelineService;

    @Before
    public void setupService() throws IOException {
        timelineService = new TimelineService(eventTypeCache,
                storageDbRepository, mock(TimelineSync.class), mock(NakadiSettings.class), timelineDbRepository,
                topicRepositoryHolder, new TransactionTemplate(mock(PlatformTransactionManager.class)),
                adminService, featureToggleService, "compacted-storage",
                new TestSchemaProviderService(localSchemaRegistry), localSchemaRegistry,
                TestUtils.getNakadiRecordMapper());
    }

    @Test(expected = NotFoundException.class)
    public void testGetTimelinesNotFound() {
        when(adminService.isAdmin(any())).thenReturn(true);
        when(eventTypeCache.getEventType(any())).thenThrow(new NoSuchEventTypeException(""));

        timelineService.getTimelines("event_type");
    }

    @Test(expected = TimelineException.class)
    public void testGetTimelinesException() {
        when(adminService.isAdmin(any())).thenReturn(true);
        when(eventTypeCache.getEventType(any())).thenThrow(new InternalNakadiException(""));

        timelineService.getTimelines("event_type");
    }

    @Test
    public void testGetTimeline() {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final Timeline timeline = Timeline.createTimeline(eventType.getName(), 0, null, "topic", new Date());
        timeline.setSwitchedAt(new Date());
        when(eventTypeCache.getTimelinesOrdered(eventType.getName()))
                .thenReturn(Collections.singletonList(timeline));

        final Timeline actualTimeline = timelineService.getActiveTimeline(eventType);
        Assert.assertEquals(timeline, actualTimeline);
    }

    @Test
    public void testGetActiveTimelinesOrderedFilters() throws Exception {
        final String eventTypeName = "my-et";

        final List<Timeline> testTimelines = range(0, 5)
                .mapToObj(x -> mock(Timeline.class))
                .collect(Collectors.toList());

        when(testTimelines.get(0).getSwitchedAt()).thenReturn(new Date());
        when(testTimelines.get(0).isDeleted()).thenReturn(false);

        when(testTimelines.get(1).getSwitchedAt()).thenReturn(new Date());
        when(testTimelines.get(1).isDeleted()).thenReturn(false);

        when(testTimelines.get(2).getSwitchedAt()).thenReturn(null);
        when(testTimelines.get(2).isDeleted()).thenReturn(false);

        when(testTimelines.get(3).getSwitchedAt()).thenReturn(new Date());
        when(testTimelines.get(3).isDeleted()).thenReturn(true);

        when(testTimelines.get(4).getSwitchedAt()).thenReturn(new Date());
        when(testTimelines.get(4).isDeleted()).thenReturn(false);

        when(eventTypeCache.getTimelinesOrdered(eq(eventTypeName))).thenReturn(testTimelines);

        final List<Timeline> expectedResult = ImmutableList.of(testTimelines.get(0), testTimelines.get(1),
                testTimelines.get(4));
        final List<Timeline> result = timelineService.getActiveTimelinesOrdered(eventTypeName);
        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void shouldDeleteTopicWhenTimelineCreationFails() throws Exception {
        final TopicRepository repository = mock(TopicRepository.class);
        when(topicRepositoryHolder.getTopicRepository(any())).thenReturn(repository);
        when(timelineDbRepository.createTimeline(any()))
                .thenThrow(new InconsistentStateException("shouldDeleteTopicWhenTimelineCreationFails"));
        when(storageDbRepository.getDefaultStorage()).thenReturn(Optional.of(mock(Storage.class)));
        try {
            timelineService.createDefaultTimeline(buildDefaultEventType(), 1);
        } catch (final InconsistentStateException e) {
        }

        Mockito.verify(repository, Mockito.times(1)).deleteTopic(any());
    }

    @Test
    public void shouldDeleteAllTimelinesWhenOneTimelineWasMarkedAsDeleted() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final Timeline t1 = Timeline.createTimeline(eventType.getName(), 1, null, "topic1", new Date());
        t1.setDeleted(true);
        t1.setSwitchedAt(new Date());
        final Timeline t2 = Timeline.createTimeline(eventType.getName(), 2, null, "topic2", new Date());
        t2.setSwitchedAt(new Date());
        when(eventTypeCache.getTimelinesOrdered(eventType.getName()))
                .thenReturn(ImmutableList.of(t1, t2));

        timelineService.deleteAllTimelinesForEventType(eventType.getName());

        Mockito.verify(timelineDbRepository, Mockito.times(2)).deleteTimeline(Mockito.any());
    }

    @Test(expected = TimelinesNotSupportedException.class)
    public void whenCreateTimelineForCompactedEventTypeThenException() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setCleanupPolicy(CleanupPolicy.COMPACT);
        when(eventTypeCache.getEventType("et1")).thenReturn(eventType);

        when(adminService.isAdmin(any())).thenReturn(true);

        timelineService.createTimeline("et1", "st1");
    }

    @Test
    public void shouldRepartitionEventTypeToAnotherStorage() {
        final EventType eventType = buildDefaultEventType();

        final Timeline t1 = Timeline.createTimeline(eventType.getName(), 1, null, "topic1", new Date());
        t1.setSwitchedAt(new Date());
        t1.setDeleted(true);

        final Storage s1 = new Storage();
        s1.setId("live");
        s1.setType(Storage.Type.KAFKA);
        final Timeline t2 = Timeline.createTimeline(eventType.getName(), 2, s1, "topic2", new Date());
        t2.setSwitchedAt(new Date());
        t2.setLatestPosition(new Timeline.KafkaStoragePosition(Lists.newArrayList(1l)));

        final Storage s2 = new Storage();
        s2.setId("backup");
        s2.setType(Storage.Type.KAFKA);
        final Timeline t3 = Timeline.createTimeline(eventType.getName(), 3, s2, "topic3", new Date());
        t3.setSwitchedAt(new Date());

        final TopicRepository topicRepository1 = mock(TopicRepository.class);
        final TopicRepository topicRepository2 = mock(TopicRepository.class);

        when(eventTypeCache.getTimelinesOrdered(eventType.getName()))
                .thenReturn(ImmutableList.of(t1, t2, t3));
        when(topicRepositoryHolder.getTopicRepository(s1))
                .thenReturn(topicRepository1);
        when(topicRepositoryHolder.getTopicRepository(s2))
                .thenReturn(topicRepository2);

        timelineService.updateTimeLineForRepartition(eventType, 2);

        Mockito.verify(topicRepository1, Mockito.times(1)).repartition("topic2", 2);
        Mockito.verify(topicRepository2, Mockito.times(1)).repartition("topic3", 2);

        Assert.assertEquals("[1, -1]", t2.getLatestPosition().toDebugString());
    }
}
