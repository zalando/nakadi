package org.zalando.nakadi.service.timeline;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
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
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.repository.TopicRepository;
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
import java.util.UUID;

public class TimelineServiceTest {

    private final SecuritySettings securitySettings = Mockito.mock(SecuritySettings.class);
    private final EventTypeCache eventTypeCache = Mockito.mock(EventTypeCache.class);
    private final StorageDbRepository storageDbRepository = Mockito.mock(StorageDbRepository.class);
    private final TimelineSync timelineSync = Mockito.mock(TimelineSync.class);
    private final NakadiSettings nakadiSettings = Mockito.mock(NakadiSettings.class);
    private final TimelineDbRepository timelineDbRepository = Mockito.mock(TimelineDbRepository.class);
    private final TopicRepositoryHolder topicRepositoryHolder = Mockito.mock(TopicRepositoryHolder.class);
    private final TransactionTemplate transactionTemplate =
            new TransactionTemplate(Mockito.mock(PlatformTransactionManager.class));

    private final TimelineService timelineService = new TimelineService(securitySettings, eventTypeCache,
            storageDbRepository, timelineSync, nakadiSettings, timelineDbRepository, topicRepositoryHolder,
            transactionTemplate, new UUIDGenerator());

    @Test
    public void testCreateTimelineFromFake() throws Exception {
        final Storage storage = new Storage();
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final TopicRepository topicRepository = Mockito.mock(TopicRepository.class);

        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getActiveTimeline(Matchers.any())).thenReturn(Optional.empty());
        Mockito.when(storageDbRepository.getStorage(Matchers.any())).thenReturn(Optional.of(storage));
        Mockito.when(topicRepositoryHolder.getTopicRepository(storage)).thenReturn(topicRepository);
        Mockito.when(topicRepository.loadTopicStatistics(Matchers.any())).thenReturn(Collections.emptyList());
        Mockito.when(eventTypeCache.getEventType(Matchers.any())).thenReturn(eventType);

        timelineService.createTimeline("event_type", "storage_id", new FullAccessClient("clientId"));

        Mockito.verify(timelineDbRepository).createTimeline(Matchers.any());
        Mockito.verify(timelineSync).startTimelineUpdate(Matchers.eq(eventType.getName()), Matchers.anyLong());
        Mockito.verify(timelineDbRepository).updateTimelime(Matchers.any());
        Mockito.verify(timelineSync).finishTimelineUpdate(eventType.getName());
    }

    @Test
    public void testCreateTimelineFromReal() throws Exception {
        final Storage storage = new Storage();
        final Timeline activeTimeline = Timeline.createTimeline("event_type", 0, storage, "topic", new Date());
        final TopicRepository topicRepository = Mockito.mock(TopicRepository.class);

        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getActiveTimeline(Matchers.any())).thenReturn(Optional.of(activeTimeline));
        Mockito.when(storageDbRepository.getStorage(Matchers.any())).thenReturn(Optional.of(storage));
        Mockito.when(topicRepositoryHolder.getTopicRepository(storage)).thenReturn(topicRepository);
        Mockito.when(topicRepositoryHolder.createStoragePosition(activeTimeline))
                .thenReturn(new Timeline.KafkaStoragePosition());
        Mockito.when(topicRepository.loadTopicStatistics(Matchers.any())).thenReturn(Collections.emptyList());
        Mockito.when(topicRepository.createTopic(Matchers.anyInt(), Matchers.anyLong())).thenReturn("topic");
        Mockito.when(eventTypeCache.getEventType(Matchers.any())).thenReturn(EventTypeTestBuilder.builder().build());

        timelineService.createTimeline("event_type", "storage_id", new FullAccessClient("clientId"));

        Mockito.verify(timelineDbRepository).createTimeline(Matchers.any());
        Mockito.verify(timelineSync)
                .startTimelineUpdate(Matchers.eq(activeTimeline.getEventType()), Matchers.anyLong());
        Mockito.verify(timelineDbRepository).updateTimelime(Matchers.eq(activeTimeline));
        Mockito.verify(timelineDbRepository).updateTimelime(Matchers.eq(activeTimeline));
        Mockito.verify(timelineSync).finishTimelineUpdate(activeTimeline.getEventType());
    }

    @Test
    public void testDeleteTimeline() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final String timelineId = "a677373f-7fc2-42bd-ab62-3d52781dcd9d";
        final Timeline timeline = Timeline.createTimeline(eventType.getName(), 0, null, "topic", new Date());
        timeline.setId(UUID.fromString(timelineId));
        final ImmutableList<Timeline> timelines = ImmutableList.of(timeline);


        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getEventType(Matchers.any())).thenReturn(eventType);
        Mockito.when(timelineDbRepository.listTimelines(eventType.getName())).thenReturn(timelines);

        timelineService.delete(eventType.getName(), timelineId, new FullAccessClient("clientId"));

        Mockito.verify(timelineSync).startTimelineUpdate(Matchers.eq(eventType.getName()), Matchers.anyLong());
        Mockito.verify(timelineDbRepository).deleteTimeline(UUID.fromString(timelineId));
        Mockito.verify(timelineSync).finishTimelineUpdate(eventType.getName());
    }

    @Test
    public void testGetTimelines() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final ImmutableList<Timeline> timelines = ImmutableList.of(
                Timeline.createTimeline("event_type", 0, null, "topic", new Date()),
                Timeline.createTimeline("event_type_1", 1, null, "topic_1", new Date()));

        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getEventType(Matchers.any())).thenReturn(eventType);
        Mockito.when(timelineDbRepository.listTimelines(eventType.getName())).thenReturn(timelines);

        final List<Timeline> actualTimelines =
                timelineService.getTimelines(eventType.getName(), new FullAccessClient("clientId"));
        Assert.assertEquals(timelines, actualTimelines);
    }

    @Test(expected = NotFoundException.class)
    public void testGetTimelinesNotFound() throws Exception {
        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getEventType(Matchers.any())).thenThrow(new NoSuchEventTypeException(""));

        timelineService.getTimelines("event_type", new FullAccessClient("clientId"));
    }

    @Test(expected = TimelineException.class)
    public void testGetTimelinesException() throws Exception {
        Mockito.when(securitySettings.getAdminClientId()).thenReturn("clientId");
        Mockito.when(eventTypeCache.getEventType(Matchers.any())).thenThrow(new InternalNakadiException(""));

        timelineService.getTimelines("event_type", new FullAccessClient("clientId"));
    }

    @Test
    public void testGetTimeline() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final Timeline timeline = Timeline.createTimeline(eventType.getName(), 0, null, "topic", new Date());

        Mockito.when(eventTypeCache.getActiveTimeline(Matchers.any())).thenReturn(Optional.of(timeline));

        final Timeline actualTimeline = timelineService.getTimeline(eventType);
        Assert.assertEquals(timeline, actualTimeline);
    }

    @Test
    public void testGetFakeTimeline() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        Mockito.when(eventTypeCache.getActiveTimeline(Matchers.any())).thenReturn(Optional.empty());
        Mockito.when(storageDbRepository.getStorage("default")).thenReturn(Optional.of(new Storage()));

        final Timeline actualTimeline = timelineService.getTimeline(eventType);
        Assert.assertTrue(actualTimeline.isFake());
    }

    @Test(expected = UnableProcessException.class)
    public void testGetTimelineUnableProcessException() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        Mockito.when(eventTypeCache.getActiveTimeline(Matchers.any())).thenReturn(Optional.empty());
        Mockito.when(storageDbRepository.getStorage("default")).thenReturn(Optional.empty());

        timelineService.getTimeline(eventType);
    }

}