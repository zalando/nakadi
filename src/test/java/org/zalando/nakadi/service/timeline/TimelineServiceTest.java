package org.zalando.nakadi.service.timeline;

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
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.security.FullAccessClient;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.Date;
import java.util.Optional;

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

}