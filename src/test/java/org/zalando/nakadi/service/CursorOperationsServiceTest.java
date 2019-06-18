package org.zalando.nakadi.service;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.kafka.KafkaFactory;
import org.zalando.nakadi.repository.kafka.KafkaSettings;
import org.zalando.nakadi.repository.kafka.KafkaTopicConfigFactory;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperSettings;
import org.zalando.nakadi.service.timeline.TimelineService;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
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
        final NakadiCursor initialCursor = NakadiCursor.of(timeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = NakadiCursor.of(timeline, "0", "0000000000000002");

        final Long distance = service.calculateDistance(initialCursor, finalCursor);

        assertThat(distance, equalTo(1L));
    }

    @Test
    public void whenCursorsOffsetsAreInvertedThenNegativeDistance() throws Exception {
        final NakadiCursor initialCursor = NakadiCursor.of(timeline, "0", "0000000000000002");
        final NakadiCursor finalCursor = NakadiCursor.of(timeline, "0", "0000000000000001");

        final Long distance = service.calculateDistance(initialCursor, finalCursor);

        assertThat(distance, equalTo(-1L));
    }

    @Test
    public void whenCursorTimelinesAreInvertedThenNegativeDistance() throws Exception {
        final Timeline initialTimeline = mockTimeline(1, 7L);
        final Timeline intermediaryTimeline = mockTimeline(2, 9L);
        final Timeline finalTimeline = mockTimeline(3, 5L);

        final NakadiCursor initialCursor = NakadiCursor.of(initialTimeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = NakadiCursor.of(finalTimeline, "0", "0000000000000003");

        mockTimelines(initialTimeline, intermediaryTimeline, finalTimeline);

        final Long distance = service.calculateDistance(finalCursor, initialCursor);

        assertThat(distance, equalTo(-20L)); // Carefully calculated value
    }

    @Test
    public void whenPartitionsDontMatch() throws Exception {
        final NakadiCursor initialCursor = NakadiCursor.of(timeline, "1", "0000000000000001");
        final NakadiCursor finalCursor = NakadiCursor.of(timeline, "0", "0000000000000002");

        expectException(initialCursor, finalCursor, CURSORS_WITH_DIFFERENT_PARTITION);
    }

    @Test
    public void whenTimelinesAreAdjacent() throws Exception {
        final Timeline initialTimeline = mockTimeline(1, 10L);
        final Timeline finalTimeline = mockOpenTimeline(2);

        mockTimelines(initialTimeline, finalTimeline);

        final NakadiCursor initialCursor = NakadiCursor.of(initialTimeline, "0", "0000000000000003");
        final NakadiCursor finalCursor = NakadiCursor.of(finalTimeline, "0", "0000000000000001");

        final Long distance = service.calculateDistance(initialCursor, finalCursor);
        assertThat(distance, equalTo(9L));
    }


    @Test
    public void whenTimelinesAreNotAdjacent() throws Exception {
        final Timeline initialTimeline = mockTimeline(1, 10L);
        final Timeline intermediaryTimeline = mockTimeline(2, 9L);
        final Timeline finalTimeline = mockTimeline(3, 0L);

        mockTimelines(initialTimeline, intermediaryTimeline, finalTimeline);

        final NakadiCursor initialCursor = NakadiCursor.of(initialTimeline, "0", "0000000000000003");
        final NakadiCursor finalCursor = NakadiCursor.of(finalTimeline, "0", "0000000000000001");

        assertThat(service.calculateDistance(initialCursor, finalCursor), equalTo(7L + 10L + 2L));
    }

    @Test(expected = InvalidCursorOperation.class)
    public void whenTimelineExpired() throws Exception {
        final Timeline expiredTimeline = timeline; // order is zero
        final Timeline initialTimeline = mockTimeline(1, 10L);
        final Timeline finalTimeline = mockTimeline(2, 0L);

        mockTimelines(initialTimeline, finalTimeline);

        final NakadiCursor initialCursor = NakadiCursor.of(expiredTimeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = NakadiCursor.of(expiredTimeline, "2", "0000000000000002");

        service.calculateDistance(initialCursor, finalCursor);
    }

    @Test
    public void calculateDistanceWhenTimelineInTheFutureAndDoesntExist() throws Exception {
        final Timeline initialTimeline = mockTimeline(1, 10L);
        final Timeline finalTimeline = mockTimeline(2, 0L);
        final Timeline futureTimeline = mockTimeline(4, 0L);

        mockTimelines(initialTimeline, finalTimeline);

        final NakadiCursor initialCursor = NakadiCursor.of(finalTimeline, "0", "0000000000000001");
        final NakadiCursor finalCursor = NakadiCursor.of(futureTimeline, "0", "0000000000000002");

        expectException(initialCursor, finalCursor, TIMELINE_NOT_FOUND);
    }

    @Test
    public void missingPartitionInTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(0, 10L);
        final Timeline intermediaryTimeline = mockTimeline(1, 9L);
        final Timeline finalTimeline = mockTimeline(2, 0L);

        mockTimelines(initialTimeline, intermediaryTimeline, finalTimeline);

        final NakadiCursor initialCursor = NakadiCursor.of(initialTimeline, "1", "0000000000000003");
        final NakadiCursor finalCursor = NakadiCursor.of(finalTimeline, "1", "0000000000000001");

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

        mockTimelines(initialTimeline, intermediaryTimeline, finalTimeline);

        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(finalTimeline, "0", "000000000000000003",
                -15L);

        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getTimeline().getOrder(), equalTo(0));
        assertThat(cursor.getOffset(), equalTo("000000000000000009"));
    }

    @Test
    public void shiftCursorToExpiredTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(1, 5L);
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
        final Timeline initialTimeline = mockTimeline(0, 10L);
        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(initialTimeline, "0", "000000000000000003",
                2L);

        mockTimelines(initialTimeline);

        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getOffset(), equalTo("000000000000000005"));
    }

    @Test
    public void shiftCursorRightToNextTimeline() throws Exception {
        final Timeline initialTimeline = mockTimeline(1, 10L);
        final Timeline nextTimeline = mockTimeline(2, 3L);
        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(initialTimeline, "0", "000000000000000003",
                9L);

        mockTimelines(initialTimeline, nextTimeline);


        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getTimeline().getOrder(), equalTo(2));
        assertThat(cursor.getOffset(), equalTo("000000000000000001"));
    }

    @Test
    public void shiftCursorForwardInTheSameTimelineOpen() {
        final Timeline initialTimeline = mockTimeline(0, 10L);
        final ShiftedNakadiCursor shiftedCursor = new ShiftedNakadiCursor(initialTimeline, "0", "000000000000000003",
                3L);

        final NakadiCursor cursor = service.unshiftCursor(shiftedCursor);

        assertThat(cursor.getOffset(), equalTo("000000000000000006"));
    }

    @Test
    public void testDistanceWithEmptyTimelines() throws Exception {
        final Timeline first = mockTimeline(1, 9L);
        final Timeline last = mockOpenTimeline(5);
        mockTimelines(first, mockTimeline(2, -1L), mockTimeline(3, -1L), mockTimeline(4, -1L), last);
        final NakadiCursor firstCursor = NakadiCursor.of(first, "0", "000000000000000001");
        final NakadiCursor lastCursor = NakadiCursor.of(last, "0", "000000000000000010");

        assertEquals(service.calculateDistance(firstCursor, lastCursor), 19L);
        assertEquals(service.calculateDistance(lastCursor, firstCursor), -19L);
    }

    @Test
    public void testShiftWithEmptyTimelines() throws Exception {
        final Timeline first = mockTimeline(1, 9L);
        final Timeline last = mockOpenTimeline(5);
        mockTimelines(first, mockTimeline(2, -1L), mockTimeline(3, -1L), mockTimeline(4, -1L), last);

        final ShiftedNakadiCursor moveForward = new ShiftedNakadiCursor(first, "0", "000000000000000001", 19);
        assertEquals(service.unshiftCursor(moveForward), NakadiCursor.of(last, "0", "000000000000000010"));

        final ShiftedNakadiCursor moveBackward = new ShiftedNakadiCursor(last, "0", "000000000000000010", -19);
        assertEquals(service.unshiftCursor(moveBackward), NakadiCursor.of(first, "0", "000000000000000001"));
    }

    @Test
    public void testShiftToInitialBegin() throws Exception {
        final Timeline first = mockTimeline(1, 1L);
        final Timeline last = mockOpenTimeline(2);
        mockTimelines(first, last);
        final ShiftedNakadiCursor moveBack = new ShiftedNakadiCursor(last, "0", "000000000000000001", -4);
        assertEquals(NakadiCursor.of(first, "0", "-1"), service.unshiftCursor(moveBack));
    }

    @Test(expected = InvalidCursorOperation.class)
    public void testShiftBeforeInitialBegin() throws Exception {
        final Timeline first = mockTimeline(1, 1L);
        mockTimelines(first);
        final ShiftedNakadiCursor moveBack = new ShiftedNakadiCursor(first, "0", "-1", -1);
        service.unshiftCursor(moveBack);
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

    private Timeline mockOpenTimeline(final int order) {
        return mockTimeline(order, null);
    }

    private Timeline mockTimeline(final int order, @Nullable final Long latestOffset) {
        final Timeline timeline = mock(Timeline.class);
        when(timeline.getOrder()).thenReturn(order);

        final Storage storage = new Storage();
        storage.setType(Storage.Type.KAFKA);
        when(timeline.getStorage()).thenReturn(storage);

        if (latestOffset == null) {
            when(timeline.isActive()).thenReturn(false);
            when(timeline.getLatestPosition()).thenReturn(null);
        } else {
            when(timeline.isActive()).thenReturn(true);
            when(timeline.getLatestPosition()).thenReturn(new Timeline.KafkaStoragePosition(
                    Collections.singletonList(latestOffset)));
        }
        when(timeline.isActive()).thenReturn(null == latestOffset);

        final TopicRepository repository = new KafkaTopicRepository(
                mock(ZooKeeperHolder.class),
                mock(KafkaFactory.class),
                mock(NakadiSettings.class),
                mock(KafkaSettings.class),
                mock(ZookeeperSettings.class),
                mock(KafkaTopicConfigFactory.class));
        when(timelineService.getTopicRepository(timeline)).thenReturn(repository);
        return timeline;
    }

    private Timeline mockTimeline(final int order) {
        return mockOpenTimeline(order);
    }

    private void mockTimelines(final Timeline... timelines) throws Exception {
        final List<Timeline> timelinesList = Arrays.asList(timelines);
        when(timelineService.getAllTimelinesOrdered(any())).thenReturn(timelinesList);
    }
}
