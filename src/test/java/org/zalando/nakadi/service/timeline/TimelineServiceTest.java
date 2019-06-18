package org.zalando.nakadi.service.timeline;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.storage.DefaultStorage;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TimelinesNotSupportedException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.NakadiAuditLogPublisher;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class TimelineServiceTest {

    private final EventTypeCache eventTypeCache = mock(EventTypeCache.class);
    private final StorageDbRepository storageDbRepository = mock(StorageDbRepository.class);
    private final AdminService adminService = mock(AdminService.class);
    private final TimelineDbRepository timelineDbRepository = mock(TimelineDbRepository.class);
    private final TopicRepositoryHolder topicRepositoryHolder = mock(TopicRepositoryHolder.class);
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private final NakadiAuditLogPublisher auditLogPublisher = mock(NakadiAuditLogPublisher.class);
    private final TimelineService timelineService = new TimelineService(eventTypeCache,
            storageDbRepository, mock(TimelineSync.class), mock(NakadiSettings.class), timelineDbRepository,
            topicRepositoryHolder, new TransactionTemplate(mock(PlatformTransactionManager.class)),
            new DefaultStorage(new Storage()), adminService, featureToggleService, "compacted-storage",
            auditLogPublisher);

    @Test(expected = NotFoundException.class)
    public void testGetTimelinesNotFound() throws Exception {
        when(adminService.isAdmin(any())).thenReturn(true);
        when(eventTypeCache.getEventType(any())).thenThrow(new NoSuchEventTypeException(""));

        timelineService.getTimelines("event_type");
    }

    @Test(expected = TimelineException.class)
    public void testGetTimelinesException() throws Exception {
        when(adminService.isAdmin(any())).thenReturn(true);
        when(eventTypeCache.getEventType(any())).thenThrow(new InternalNakadiException(""));

        timelineService.getTimelines("event_type");
    }

    @Test
    public void testGetTimeline() throws Exception {
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

}
