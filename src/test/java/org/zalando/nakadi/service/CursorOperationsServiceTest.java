package org.zalando.nakadi.service;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.kafka.KafkaFactory;
import org.zalando.nakadi.repository.kafka.KafkaSettings;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.CURSORS_WITH_DIFFERENT_PARTITION;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.PARTITION_NOT_FOUND;
import static org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation.Reason.TIMELINE_NOT_FOUND;

public class CursorOperationsServiceTest {
    private TimelineService timelineService = mock(TimelineService.class);
    private CursorOperationsService service = new CursorOperationsService(timelineService);
    private final Timeline timeline = mockTimeline(0);

    @Test
    public void whenCursorsAreInTheSameTimeline() throws Exception {
        final NakadiCursor initialCursor = new NakadiCursor(timeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = new NakadiCursor(timeline, "0", "0000000000000002");

        final Long distance = service.calculateDistance(initialCursor, finalCursor);

        assertThat(distance, equalTo(1L));
    }

    @Test
    public void whenCursorsOffsetsAreInvertedThenNegativeDistance() throws Exception {
        final NakadiCursor initialCursor = new NakadiCursor(timeline, "0", "0000000000000002");
        final NakadiCursor finalCursor = new NakadiCursor(timeline, "0", "0000000000000001");

        final Long distance = service.calculateDistance(initialCursor, finalCursor);

        assertThat(distance, equalTo(-1L));
    }

    @Test
    public void whenCursorTimelinesAreInvertedThenNegativeDistance() throws Exception {
        final Timeline initialTimeline = mockTimeline(2, 7);
        final Timeline intermediaryTimeline = mockTimeline(1, 9L);
        final Timeline finalTimeline = mockTimeline(0, 5);

        final NakadiCursor initialCursor = new NakadiCursor(initialTimeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = new NakadiCursor(finalTimeline, "0", "0000000000000003");

        mockActiveTimelines(initialTimeline, intermediaryTimeline, finalTimeline);

        final Long distance = service.calculateDistance(initialCursor, finalCursor);

        assertThat(distance, equalTo(-14L));
    }

    @Test
    public void whenPartitionsDontMatch() throws Exception {
        final NakadiCursor initialCursor = new NakadiCursor(timeline, "1", "0000000000000001");
        final NakadiCursor finalCursor = new NakadiCursor(timeline, "0", "0000000000000002");

        expectException(initialCursor, finalCursor, CURSORS_WITH_DIFFERENT_PARTITION);
    }

    @Test
    public void whenTimelinesAreAdjacent() throws Exception {
        final Timeline initialTimeline = mockTimeline(0, 10);
        final Timeline finalTimeline = mockTimeline(1, true);

        final NakadiCursor initialCursor = new NakadiCursor(initialTimeline, "0", "0000000000000003");
        final NakadiCursor finalCursor = new NakadiCursor(finalTimeline, "0", "0000000000000001");

        final Long distance = service.calculateDistance(initialCursor, finalCursor);
        assertThat(distance, equalTo(9L));
    }


    @Test
    public void whenTimelinesAreNotAdjacent() throws Exception {
        final Timeline initialTimeline = mockTimeline(0, 10L);
        final Timeline intermediaryTimeline = mockTimeline(1, 9L);
        final Timeline finalTimeline = mockTimeline(2, 0L);

        mockActiveTimelines(initialTimeline, intermediaryTimeline, finalTimeline);

        final NakadiCursor initialCursor = new NakadiCursor(initialTimeline, "0", "0000000000000003");
        final NakadiCursor finalCursor = new NakadiCursor(finalTimeline, "0", "0000000000000001");

        assertThat(service.calculateDistance(initialCursor, finalCursor), equalTo(7L + 10L + 2L));
    }

    @Test(expected = InvalidCursorOperation.class)
    public void whenTimelineExpired() throws Exception {
        final Timeline expiredTimeline = timeline; // order is zero
        final Timeline initialTimeline = mockTimeline(1, 10L);
        final Timeline finalTimeline = mockTimeline(2, 0L);

        mockActiveTimelines(initialTimeline, finalTimeline);

        final NakadiCursor initialCursor = new NakadiCursor(expiredTimeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = new NakadiCursor(expiredTimeline, "2", "0000000000000002");

        service.calculateDistance(initialCursor, finalCursor);
    }

    @Test
    public void calculateDistanceWhenTimelineInTheFutureAndDoesntExist() throws Exception {
        final Timeline initialTimeline = mockTimeline(1, 10L);
        final Timeline finalTimeline = mockTimeline(2, 0L);
        final Timeline futureTimeline = mockTimeline(4, 0L);

        mockActiveTimelines(initialTimeline, finalTimeline);

        final NakadiCursor initialCursor = new NakadiCursor(finalTimeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = new NakadiCursor(futureTimeline, "0", "0000000000000002");

        expectException(initialCursor, finalCursor, TIMELINE_NOT_FOUND);
    }

    @Test
    public void missingPartitionInTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(0, 10L);
        final Timeline intermediaryTimeline = mockTimeline(1, 9L);
        final Timeline finalTimeline = mockTimeline(2, 0L);

        mockActiveTimelines(initialTimeline, intermediaryTimeline, finalTimeline);

        final NakadiCursor initialCursor = new NakadiCursor(initialTimeline, "1", "0000000000000003");
        final NakadiCursor finalCursor = new NakadiCursor(finalTimeline, "1", "0000000000000001");

        expectException(initialCursor, finalCursor, PARTITION_NOT_FOUND);
    }

    @Test
    public void shiftCursorBackInTheSameTimelineClosed() {
        final Timeline initialTimeline = mockTimeline(0, 10L);
        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(initialTimeline, "0", "000000000000000003",
                -3L);

        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getOffset(), equalTo("000000000000000000"));
    }

    @Test
    public void shiftCursorBackToPreviousTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(0, 10L);
        final Timeline intermediaryTimeline = mockTimeline(1, 9L);
        final Timeline finalTimeline = mockTimeline(2, 9L);

        when(timelineService.getActiveTimelinesOrdered(any()))
                .thenReturn(Lists.newArrayList(initialTimeline, intermediaryTimeline, finalTimeline));

        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(finalTimeline, "0", "000000000000000003",
                -15L);

        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getTimeline().getOrder(), equalTo(0));
        assertThat(cursor.getOffset(), equalTo("000000000000000009"));
    }

    @Test
    public void shiftCursorToExpiredTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(0);
        final Timeline finalTimeline = mockTimeline(1);

        when(timelineService.getActiveTimelinesOrdered(any()))
                .thenReturn(Lists.newArrayList(initialTimeline, finalTimeline));

        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(finalTimeline, "0", "000000000000000003",
                -15L);

        try {
            service.unshiftCursor(shiftedCursor);
            fail();
        } catch (final InvalidCursorOperation e) {
            assertThat(e.getReason(), equalTo(TIMELINE_NOT_FOUND));
        } catch (final Exception e) {
            fail();
        }
    }

    @Test
    public void shiftCursorToTheRightSameClosedTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(0, 10);
        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(initialTimeline, "0", "000000000000000003",
                2L);

        mockActiveTimelines(initialTimeline);

        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getOffset(), equalTo("000000000000000005"));
    }

    @Test
    public void shiftCursorRightToNextTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(0, 10);
        final Timeline nextTimeline = mockTimeline(1, 3);
        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(initialTimeline, "0", "000000000000000003",
                9L);

        mockActiveTimelines(initialTimeline, nextTimeline);


        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getTimeline().getOrder(), equalTo(1));
        assertThat(cursor.getOffset(), equalTo("000000000000000001"));
    }

    @Test
    public void shiftCursorForwardInTheSameTimelineOpen() {
        final Timeline initialTimeline = mockTimeline(0, 10);
        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(initialTimeline, "0", "000000000000000003",
                3L);

        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getOffset(), equalTo("000000000000000006"));
    }

    private void expectException(final NakadiCursor initialCursor, final NakadiCursor finalCursor,
                                 final InvalidCursorOperation.Reason invertedOffsetOrder) {
        try {
            service.calculateDistance(initialCursor, finalCursor);
            fail();
        } catch (final InvalidCursorOperation e) {
            assertThat(e.getReason(), equalTo(invertedOffsetOrder));
        } catch (final Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    private Timeline mockTimeline(final int order, final boolean isActive) {
        return mockTimeline(order, 0);
    }

    private Timeline mockTimeline(final int order, final long latestOffset) {
        final Timeline timeline = mock(Timeline.class);
        when(timeline.getOrder()).thenReturn(order);
        final Timeline.KafkaStoragePosition positions = mock(Timeline.KafkaStoragePosition.class);
        when(positions.getOffsets()).thenReturn(Lists.newArrayList(latestOffset));
        when(timeline.getLatestPosition()).thenReturn(positions);
        when(timeline.isActive()).thenReturn(latestOffset == 0);

        final TopicRepository repository = new KafkaTopicRepository(
                mock(ZooKeeperHolder.class),
                mock(KafkaFactory.class),
                mock(NakadiSettings.class),
                mock(KafkaSettings.class),
                mock(ZookeeperSettings.class),
                mock(UUIDGenerator.class));
        when(timelineService.getTopicRepository(timeline)).thenReturn(repository);
        return timeline;
    }

    private Timeline mockTimeline(final int order) {
        return mockTimeline(order, true);
    }

    private void mockActiveTimelines(final Timeline ...timelines) throws Exception {
        final List<Timeline> timelinesList = Lists.newArrayList();
        for (final Timeline t : timelines) {
            timelinesList.add(t);
        }

        when(timelineService.getActiveTimelinesOrdered(any())).thenReturn(timelinesList);
    }
}