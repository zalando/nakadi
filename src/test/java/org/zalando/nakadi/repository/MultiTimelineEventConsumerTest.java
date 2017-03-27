package org.zalando.nakadi.repository;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.kafka.KafkaCursor;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MultiTimelineEventConsumerTest {

    @Test
    public void testExistingTimelineIsSwitchedDuringConsumption()
            throws NakadiException, IOException, InvalidCursorException {
        final String et1 = "et1";
        final String testClientId = "testClientId";
        final String partition = "0";

        final Timeline first = new Timeline(et1, 1, null, "et1t1", new Date());
        final Timeline second = new Timeline(et1, 2, null, "et1t2", new Date());
        final List<NakadiCursor> startFrom = ImmutableList.of(
                new NakadiCursor(first, partition, KafkaCursor.toNakadiOffset(1)));

        final TimelineSync timelineSync = mock(TimelineSync.class);
        final TimelineService timelineService = mock(TimelineService.class);
        final TimelineSync.ListenerRegistration registration = mock(TimelineSync.ListenerRegistration.class);
        when(timelineSync.registerTimelineChangeListener(eq(et1), any())).thenReturn(registration);

        // Setup mocks for first timeline
        final NakadiCursor lastCursorInFirstTimeline = new KafkaCursor("et1t1", 0, 2).toNakadiCursor(first);
        first.setLatestPosition(new Timeline.KafkaStoragePosition(ImmutableList.of(2L)));
        final TopicRepository firstTopicRepo = mock(TopicRepository.class);
        final EventConsumer firstEventConsumer = mock(EventConsumer.class);
        when(firstEventConsumer.readEvents())
                .thenReturn(Collections.singletonList(new ConsumedEvent("text", lastCursorInFirstTimeline)));
        when(firstTopicRepo.createEventConsumer(eq(testClientId), eq(startFrom))).thenReturn(firstEventConsumer);
        when(timelineService.getTopicRepository(eq(first))).thenReturn(firstTopicRepo);

        // Setup mocks for second timeline
        final TopicRepository secondTopicRepository = mock(TopicRepository.class);
        final EventConsumer secondEventConsumer = mock(EventConsumer.class);
        final PartitionStatistics statistics = mock(PartitionStatistics.class);
        final NakadiCursor beforeFirstInSecondTimeline =
                new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(-1));
        when(statistics.getBeforeFirst()).thenReturn(beforeFirstInSecondTimeline);
        when(secondTopicRepository.loadPartitionStatistics(second, partition)).thenReturn(Optional.of(statistics));
        when(timelineService.getActiveTimelinesOrdered(et1)).thenReturn(ImmutableList.of(first, second));
        when(secondTopicRepository.createEventConsumer(
                eq(testClientId),
                eq(ImmutableList.of(beforeFirstInSecondTimeline))))
                .thenReturn(secondEventConsumer);
        when(timelineService.getTopicRepository(eq(second))).thenReturn(secondTopicRepository);
        when(secondEventConsumer.readEvents()).thenReturn(ImmutableList.of(
                new ConsumedEvent("test2", new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(0))),
                new ConsumedEvent("test3", new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(1)))),
                Collections.emptyList()
        );

        final MultiTimelineEventConsumer consumer = new MultiTimelineEventConsumer(
                testClientId, timelineService, timelineSync);
        try {
            // check that it works without assignment
            Assert.assertTrue(consumer.readEvents().isEmpty());

            // Now assign to new cursors
            consumer.reassign(startFrom);

            final List<ConsumedEvent> fromFirstTimeline = consumer.readEvents();
            Assert.assertFalse(fromFirstTimeline.isEmpty());
            Assert.assertEquals(lastCursorInFirstTimeline, fromFirstTimeline.get(0).getPosition());
            final List<ConsumedEvent> fromSecondTimeline = consumer.readEvents();
            Assert.assertEquals(2, fromSecondTimeline.size());
            Assert.assertEquals("test2", fromSecondTimeline.get(0).getEvent());
            Assert.assertEquals("test3", fromSecondTimeline.get(1).getEvent());
        } finally {
            consumer.close();
        }

        verify(firstEventConsumer, times(1)).readEvents();
        verify(secondEventConsumer, times(1)).readEvents();
        // first call - from reassign, second call - while switch.
        verify(timelineService, times(2)).getActiveTimelinesOrdered(eq(et1));
    }

    @Test
    public void testTimelineRefreshProcessedAndDataIsStreamedFromNextTimeline()
            throws NakadiException, InvalidCursorException, IOException {
        final String et1 = "et1";
        final String testClientId = "testClientId";
        final String partition = "0";

        final Timeline first = new Timeline(et1, 1, null, "et1t1", new Date());
        final List<NakadiCursor> startFrom = ImmutableList.of(
                new NakadiCursor(first, partition, KafkaCursor.toNakadiOffset(1)));

        final TimelineSync timelineSync = mock(TimelineSync.class);
        final TimelineService timelineService = mock(TimelineService.class);
        final TimelineSync.ListenerRegistration registration = mock(TimelineSync.ListenerRegistration.class);
        when(timelineSync.registerTimelineChangeListener(eq(et1), any())).thenReturn(registration);

        // Setup mocks for first timeline
        final NakadiCursor lastCursorInFirstTimeline = new KafkaCursor("et1t1", 0, 2).toNakadiCursor(first);

        final TopicRepository firstTopicRepo = mock(TopicRepository.class);
        final EventConsumer firstEventConsumer = mock(EventConsumer.class);
        when(firstEventConsumer.readEvents()).thenReturn(
                Collections.singletonList(new ConsumedEvent("text", lastCursorInFirstTimeline)),
                Collections.emptyList());
        when(firstTopicRepo.createEventConsumer(eq(testClientId), eq(startFrom))).thenReturn(firstEventConsumer);
        when(timelineService.getTopicRepository(eq(first))).thenReturn(firstTopicRepo);

        final Timeline firstAfterUpdate = new Timeline(et1, 1, null, "et1t1", new Date());
        firstAfterUpdate.setLatestPosition(new Timeline.KafkaStoragePosition(ImmutableList.of(2L)));
        final Timeline second = new Timeline(et1, 2, null, "et1t2", new Date());
        when(timelineService.getActiveTimelinesOrdered(eq(et1))).thenReturn(
                ImmutableList.of(first),
                ImmutableList.of(firstAfterUpdate, second)
        );
        final TopicRepository secondTopicRepository = mock(TopicRepository.class);
        final EventConsumer secondEventConsumer = mock(EventConsumer.class);
        final PartitionStatistics statistics = mock(PartitionStatistics.class);
        final NakadiCursor beforeFirstInSecondTimeline =
                new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(-1));
        when(statistics.getBeforeFirst()).thenReturn(beforeFirstInSecondTimeline);
        when(secondTopicRepository.loadPartitionStatistics(second, partition)).thenReturn(Optional.of(statistics));
        when(secondTopicRepository.createEventConsumer(
                eq(testClientId),
                eq(ImmutableList.of(beforeFirstInSecondTimeline))))
                .thenReturn(secondEventConsumer);
        when(timelineService.getTopicRepository(eq(second))).thenReturn(secondTopicRepository);
        when(secondEventConsumer.readEvents()).thenReturn(
                ImmutableList.of(
                        new ConsumedEvent("test2", new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(0))),
                        new ConsumedEvent("test3", new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(1)))),
                Collections.emptyList()
        );

        final MultiTimelineEventConsumer consumer = new MultiTimelineEventConsumer(
                testClientId, timelineService, timelineSync);
        try {
            // Now assign to new cursors
            consumer.reassign(startFrom);

            final List<ConsumedEvent> fromFirstTimeline = consumer.readEvents();
            Assert.assertFalse(fromFirstTimeline.isEmpty());
            Assert.assertEquals(lastCursorInFirstTimeline, fromFirstTimeline.get(0).getPosition());

            final List<ConsumedEvent> fromFristTimelineAbsent = consumer.readEvents();
            Assert.assertTrue(fromFristTimelineAbsent.isEmpty());
            // Suppose that timeline change occurred now
            consumer.onTimelineChange(et1);
            // One must switch to next timeline.

            final List<ConsumedEvent> fromSecond1 = consumer.readEvents();
            Assert.assertFalse(fromSecond1.isEmpty());
            Assert.assertEquals("test2", fromSecond1.get(0).getEvent());
            Assert.assertEquals("test3", fromSecond1.get(1).getEvent());

        } finally {
            consumer.close();
        }

        verify(firstEventConsumer, times(2)).readEvents();
        verify(timelineService, times(2)).getActiveTimelinesOrdered(eq(et1));
    }


}