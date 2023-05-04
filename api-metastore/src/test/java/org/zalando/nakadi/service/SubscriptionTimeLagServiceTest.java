package org.zalando.nakadi.service;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.service.timeline.HighLevelConsumer;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionTimeLagServiceTest {

    private static final long FAKE_EVENT_TIMESTAMP = 478220400000L;

    private NakadiCursorComparator cursorComparator;
    private TimelineService timelineService;
    private NakadiSettings nakadiSettings;

    @Before
    public void setUp() {
        timelineService = mock(TimelineService.class);
        cursorComparator = mock(NakadiCursorComparator.class);
        nakadiSettings = mock(NakadiSettings.class);
        when(nakadiSettings.getKafkaTimeLagCheckerConsumerPoolSize()).thenReturn(2);
    }

    @Test
    public void testTimeLagsForTailAndNotTailPositions() throws InvalidCursorException {

        final HighLevelConsumer eventConsumer = mock(HighLevelConsumer.class);
        final Timeline timeline = mock(Timeline.class);
        when(timeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));
        when(eventConsumer.readEvents())
                .thenAnswer(invocation ->
                        ImmutableList.of(
                                new ConsumedEvent(
                                        null, NakadiCursor.of(timeline, "", ""), FAKE_EVENT_TIMESTAMP, null)));

        when(timelineService.createEventConsumer(anyString())).thenReturn(eventConsumer);

        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);

        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");
        final NakadiCursor committedCursor2 = NakadiCursor.of(et1Timeline, "p2", "o2");

        final PartitionEndStatistics endStats1 = mockEndStats(NakadiCursor.of(et1Timeline, "p1", "o1"));
        final PartitionEndStatistics endStats2 = mockEndStats(NakadiCursor.of(et1Timeline, "p2", "o3"));

        // mock first committed cursor to be at the tail - the expected time lag should be 0
        when(cursorComparator.compare(committedCursor1, endStats1.getLast())).thenReturn(0);

        // mock second committed cursor to be lower than tail - the expected time lag should be > 0
        when(cursorComparator.compare(committedCursor2, endStats2.getLast())).thenReturn(-1);

        final SubscriptionTimeLagService timeLagService = new SubscriptionTimeLagService(
                timelineService, cursorComparator, new MetricRegistry(), nakadiSettings);
        final Map<EventTypePartition, Duration> timeLags = timeLagService.getTimeLags(
                ImmutableList.of(committedCursor1, committedCursor2),
                ImmutableList.of(endStats1, endStats2));

        assertThat(timeLags.entrySet(), hasSize(2));
        assertThat(timeLags.get(new EventTypePartition("et1", "p1")), equalTo(Duration.ZERO));
        assertThat(timeLags.get(new EventTypePartition("et1", "p2")), greaterThan(Duration.ZERO));
    }


    @Test
    public void whenNoSubscriptionThenReturnSizeZeroMap() {
        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);
        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");

        when(timelineService.createEventConsumer(anyString())).thenReturn(mock(HighLevelConsumer.class));
        final SubscriptionTimeLagService timeLagService = new SubscriptionTimeLagService(
                timelineService, cursorComparator, new MetricRegistry(), nakadiSettings);
        final Map<EventTypePartition, Duration> result = timeLagService.getTimeLags
                (ImmutableList.of(committedCursor1), ImmutableList.of());
        assertThat(result.size(), is(0));
    }

    private PartitionEndStatistics mockEndStats(final NakadiCursor nakadiCursor) {
        final PartitionEndStatistics endStats = mock(PartitionEndStatistics.class);
        when(endStats.getLast()).thenReturn(nakadiCursor);
        return endStats;
    }
}
