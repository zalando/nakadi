package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.service.subscription.SubscriptionTimeLagService;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionTimeLagServiceTest {

    private static final long FAKE_EVENT_TIMESTAMP = 478220400000L;

    private NakadiCursorComparator cursorComparator;
    private SubscriptionTimeLagService timeLagService;
    private TimelineService timelineService;

    @Before
    public void setUp() throws Exception {
        timelineService = mock(TimelineService.class);

        cursorComparator = mock(NakadiCursorComparator.class);
        timeLagService = new SubscriptionTimeLagService(timelineService, cursorComparator);
    }

    @Test
    public void testTimeLagsForTailAndNotTailPositions() throws InternalNakadiException, InvalidCursorException {

        final EventConsumer eventConsumer = mock(EventConsumer.class);
        final Timeline timeline = mock(Timeline.class);
        when(timeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));
        when(eventConsumer.readEvents()).thenAnswer(invocation ->
                ImmutableList.of(new ConsumedEvent(null, NakadiCursor.of(timeline, "", ""), FAKE_EVENT_TIMESTAMP)));

        when(timelineService.createEventConsumer(any(), any())).thenReturn(eventConsumer);

        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);

        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");
        final NakadiCursor committedCursor2 = NakadiCursor.of(et1Timeline, "p2", "o2");

        final PartitionEndStatistics endStats1 = mockEndStats(NakadiCursor.of(et1Timeline, "p1", "o1"));
        final PartitionEndStatistics endStats2 = mockEndStats(NakadiCursor.of(et1Timeline, "p2", "o3"));

        // mock first committed cursor to be at the tail - the expected time lag should be 0
        when(cursorComparator.compare(committedCursor1, endStats1.getLast())).thenReturn(0);

        // mock second committed cursor to be lower than tail - the expected time lag should be > 0
        when(cursorComparator.compare(committedCursor2, endStats2.getLast())).thenReturn(-1);

        final Map<EventTypePartition, Duration> timeLags = timeLagService.getTimeLags(
                ImmutableList.of(committedCursor1, committedCursor2),
                ImmutableList.of(endStats1, endStats2));

        assertThat(timeLags.entrySet(), hasSize(2));
        assertThat(timeLags.get(new EventTypePartition("et1", "p1")), equalTo(Duration.ZERO));
        assertThat(timeLags.get(new EventTypePartition("et1", "p2")), greaterThan(Duration.ZERO));
    }


    @Test(expected = InconsistentStateException.class)
    @SuppressWarnings("unchecked")
    public void whenNakadiExceptionThenInconsistentStateExceptionIsThrown()
            throws InternalNakadiException, InvalidCursorException {
        when(timelineService.createEventConsumer(any(), any())).thenThrow(InternalNakadiException.class);

        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);
        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");

        timeLagService.getTimeLags(ImmutableList.of(committedCursor1), ImmutableList.of());
    }

    @Test(expected = ErrorGettingCursorTimeLagException.class)
    @SuppressWarnings("unchecked")
    public void whenInvalidCursorThenErrorGettingCursorTimeLagExceptionIsThrown()
            throws InternalNakadiException, InvalidCursorException {
        when(timelineService.createEventConsumer(any(), any())).thenThrow(InvalidCursorException.class);

        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);
        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");

        timeLagService.getTimeLags(ImmutableList.of(committedCursor1), ImmutableList.of());
    }

    private PartitionEndStatistics mockEndStats(final NakadiCursor nakadiCursor) {
        final PartitionEndStatistics endStats = mock(PartitionEndStatistics.class);
        when(endStats.getLast()).thenReturn(nakadiCursor);
        return endStats;
    }
}
