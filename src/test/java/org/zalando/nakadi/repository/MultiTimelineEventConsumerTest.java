package org.zalando.nakadi.repository;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
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
        when(firstEventConsumer.readEvent())
                .thenReturn(Optional.of(new ConsumedEvent("text", lastCursorInFirstTimeline)));
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
        when(secondEventConsumer.readEvent()).thenReturn(
                Optional.of(
                        new ConsumedEvent("test2", new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(0)))),
                Optional.of(
                        new ConsumedEvent("test3", new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(1)))),
                Optional.empty()
        );

        final MultiTimelineEventConsumer consumer = new MultiTimelineEventConsumer(
                testClientId, timelineService, timelineSync);
        try {
            // check that it works without assignment
            Assert.assertFalse(consumer.readEvent().isPresent());

            // Now assign to new cursors
            consumer.reassign(startFrom);

            final Optional<ConsumedEvent> fromFirstTimeline = consumer.readEvent();
            Assert.assertTrue(fromFirstTimeline.isPresent());
            Assert.assertEquals(lastCursorInFirstTimeline, fromFirstTimeline.get().getPosition());
            final Optional<ConsumedEvent> fromSecondTimeline0 = consumer.readEvent();
            Assert.assertTrue(fromSecondTimeline0.isPresent());
            Assert.assertEquals("test2", fromSecondTimeline0.get().getEvent());
            final Optional<ConsumedEvent> fromSecondTimeline1 = consumer.readEvent();
            Assert.assertTrue(fromSecondTimeline1.isPresent());
            Assert.assertEquals("test3", fromSecondTimeline1.get().getEvent());
            final Optional<ConsumedEvent> fromSecondTimeline2 = consumer.readEvent();
            Assert.assertFalse(fromSecondTimeline2.isPresent());
        } finally {
            consumer.close();
        }

        verify(firstEventConsumer, times(1)).readEvent();
        verify(secondEventConsumer, times(3)).readEvent();
        verify(timelineService, times(1)).getActiveTimelinesOrdered(eq(et1));
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
        when(firstEventConsumer.readEvent()).thenReturn(
                Optional.of(new ConsumedEvent("text", lastCursorInFirstTimeline)),
                Optional.empty());
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
        when(secondEventConsumer.readEvent()).thenReturn(
                Optional.of(new ConsumedEvent("test2",
                        new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(0)))),
                Optional.of(new ConsumedEvent("test3",
                        new NakadiCursor(second, partition, KafkaCursor.toNakadiOffset(1)))),
                Optional.empty()
        );

        final MultiTimelineEventConsumer consumer = new MultiTimelineEventConsumer(
                testClientId, timelineService, timelineSync);
        try {
            // Now assign to new cursors
            consumer.reassign(startFrom);

            final Optional<ConsumedEvent> fromFirstTimeline = consumer.readEvent();
            Assert.assertTrue(fromFirstTimeline.isPresent());
            Assert.assertEquals(lastCursorInFirstTimeline, fromFirstTimeline.get().getPosition());

            final Optional<ConsumedEvent> fromFristTimelineAbsent = consumer.readEvent();
            Assert.assertFalse(fromFristTimelineAbsent.isPresent());
            // Suppose that timeline change occurred now
            consumer.onTimelineChange(et1);
            // One must switch to next timeline.

            final Optional<ConsumedEvent> fromSecond1 = consumer.readEvent();
            Assert.assertTrue(fromSecond1.isPresent());
            Assert.assertEquals("test2", fromSecond1.get().getEvent());

            final Optional<ConsumedEvent> fromSecond2 = consumer.readEvent();
            Assert.assertTrue(fromSecond2.isPresent());
            Assert.assertEquals("test3", fromSecond2.get().getEvent());

        } finally {
            consumer.close();
        }

        verify(firstEventConsumer, times(2)).readEvent();
        verify(timelineService, times(2)).getActiveTimelinesOrdered(eq(et1));
    }


}