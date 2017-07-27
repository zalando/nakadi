package org.zalando.nakadi.service.timeline;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NotFoundException;
import org.zalando.nakadi.exceptions.TimelineException;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.security.FullAccessClient;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

public class TimelineServiceTest {

    private final SecuritySettings securitySettings = Mockito.mock(SecuritySettings.class);
    private final EventTypeCache eventTypeCache = Mockito.mock(EventTypeCache.class);
    private final StorageDbRepository storageDbRepository = Mockito.mock(StorageDbRepository.class);

    private final TimelineService timelineService = new TimelineService(securitySettings, eventTypeCache,
            storageDbRepository, Mockito.mock(TimelineSync.class), Mockito.mock(NakadiSettings.class),
            Mockito.mock(TimelineDbRepository.class), Mockito.mock(TopicRepositoryHolder.class),
            new TransactionTemplate(Mockito.mock(PlatformTransactionManager.class)), new UUIDGenerator(),
            new Storage());

    @Test(expected = NotFoundException.class)
    public void testGetTimelinesNotFound() throws Exception {
        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getEventType(any())).thenThrow(new NoSuchEventTypeException(""));

        timelineService.getTimelines("event_type", new FullAccessClient("clientId"));
    }

    @Test(expected = TimelineException.class)
    public void testGetTimelinesException() throws Exception {
        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getEventType(any())).thenThrow(new InternalNakadiException(""));

        timelineService.getTimelines("event_type", new FullAccessClient("clientId"));
    }

    @Test
    public void testGetTimeline() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final Timeline timeline = Timeline.createTimeline(eventType.getName(), 0, null, "topic", new Date());

        Mockito.when(eventTypeCache.getActiveTimeline(any())).thenReturn(Optional.of(timeline));

        final Timeline actualTimeline = timelineService.getTimeline(eventType);
        Assert.assertEquals(timeline, actualTimeline);
    }

    @Test
    public void testGetFakeTimeline() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        Mockito.when(eventTypeCache.getActiveTimeline(any())).thenReturn(Optional.empty());
        Mockito.when(storageDbRepository.getStorage("default")).thenReturn(Optional.of(new Storage()));

        final Timeline actualTimeline = timelineService.getTimeline(eventType);
        Assert.assertTrue(actualTimeline.isFake());
    }

    @Test
    public void testGetActiveTimelinesOrderedUseFake() throws Exception {
        final String eventTypeName = "my-et";
        final EventType evenType = Mockito.mock(EventType.class);
        Mockito.when(eventTypeCache.getEventType(eq(eventTypeName))).thenReturn(evenType);
        Mockito.when(eventTypeCache.getTimelinesOrdered(eq(eventTypeName))).thenReturn(Collections.emptyList());
        final Storage storage = Mockito.mock(Storage.class);
        Mockito.when(storageDbRepository.getStorage(any())).thenReturn(Optional.of(storage));

        final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
        Assert.assertEquals(1, timelines.size());
        final Timeline timeline = timelines.get(0);
        Assert.assertTrue(timeline.isFake());
    }

    @Test
    public void testGetActiveTimelinesOrderedFilters() throws Exception {
        final String eventTypeName = "my-et";

        final List<Timeline> testTimelines = range(0, 5)
                .mapToObj(x -> Mockito.mock(Timeline.class))
                .collect(Collectors.toList());

        Mockito.when(testTimelines.get(0).getSwitchedAt()).thenReturn(new Date());
        Mockito.when(testTimelines.get(0).isDeleted()).thenReturn(false);

        Mockito.when(testTimelines.get(1).getSwitchedAt()).thenReturn(new Date());
        Mockito.when(testTimelines.get(1).isDeleted()).thenReturn(false);

        Mockito.when(testTimelines.get(2).getSwitchedAt()).thenReturn(null);
        Mockito.when(testTimelines.get(2).isDeleted()).thenReturn(false);

        Mockito.when(testTimelines.get(3).getSwitchedAt()).thenReturn(new Date());
        Mockito.when(testTimelines.get(3).isDeleted()).thenReturn(true);

        Mockito.when(testTimelines.get(4).getSwitchedAt()).thenReturn(new Date());
        Mockito.when(testTimelines.get(4).isDeleted()).thenReturn(false);

        Mockito.when(eventTypeCache.getTimelinesOrdered(eq(eventTypeName))).thenReturn(testTimelines);

        final List<Timeline> expectedResult = ImmutableList.of(testTimelines.get(0), testTimelines.get(1),
                testTimelines.get(4));
        final List<Timeline> result = timelineService.getActiveTimelinesOrdered(eventTypeName);
        Assert.assertEquals(expectedResult, result);
    }

}