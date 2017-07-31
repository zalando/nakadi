package org.zalando.nakadi.repository.db;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.EventBodyMustRespectSchema;
import org.zalando.nakadi.validation.EventMetadataValidationStrategy;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.ValidationStrategy;

import java.util.Collections;
import java.util.List;

public class EventTypeCacheTest {
    @BeforeClass
    public static void initValidation() {
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema(
                new JsonSchemaEnrichment()
        ));
        ValidationStrategy.register(EventMetadataValidationStrategy.NAME, new EventMetadataValidationStrategy());
    }

    @Test
    public void testEventTypesPreloaded() throws Exception {
        final EventTypeRepository etRepo = Mockito.mock(EventTypeRepository.class);
        final TimelineDbRepository timelineRepository = Mockito.mock(TimelineDbRepository.class);
        final ZooKeeperHolder zkHolder = Mockito.mock(ZooKeeperHolder.class);
        final TimelineSync timelineSync = Mockito.mock(TimelineSync.class);

        final EventType et = TestUtils.buildDefaultEventType();
        Mockito.when(etRepo.list()).thenReturn(Collections.singletonList(et));
        final Timeline timeline = TestUtils.buildTimeline(et.getName());
        final List<Timeline> timelines = Collections.singletonList(timeline);
        Mockito.when(timelineRepository.listTimelinesOrdered()).thenReturn(timelines);
        Mockito.when(timelineSync.registerTimelineChangeListener(Matchers.eq(et.getName()), Mockito.any()))
                .thenReturn(() -> {});
        final EventTypeCache eventTypeCache = new EventTypeCache(etRepo, timelineRepository, zkHolder,
                null, timelineSync) {
            @Override
            public void created(final String name) throws Exception {
                // ignore this call, because mocking is too complex
            }
        };

        Assert.assertSame(et, eventTypeCache.getEventType(et.getName()));

        Mockito.verify(etRepo, Mockito.times(0)).findByName(Mockito.any());
        Mockito.verify(etRepo, Mockito.times(1)).list();

        Assert.assertEquals(timelines, eventTypeCache.getTimelinesOrdered(et.getName()));

        Mockito.verify(timelineRepository, Mockito.times(0)).listTimelinesOrdered(Mockito.any());
        Mockito.verify(timelineRepository, Mockito.times(1)).listTimelinesOrdered();
    }
}
