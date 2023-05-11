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

    private TimelineService timelineService;
    private NakadiSettings nakadiSettings;

    @Before
    public void setUp() {
        timelineService = mock(TimelineService.class);
        nakadiSettings = mock(NakadiSettings.class);
        when(nakadiSettings.getKafkaTimeLagCheckerConsumerPoolSize()).thenReturn(2);
    }

    @Test
    public void testTimeLagsForTailAndNotTailPositions() throws InvalidCursorException {

        final HighLevelConsumer eventConsumer1 = mock(HighLevelConsumer.class);
        final HighLevelConsumer eventConsumer2 = mock(HighLevelConsumer.class);

        final Timeline timeline = mock(Timeline.class);
        when(timeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));

        when(eventConsumer1.readEvents()).thenAnswer(invocation -> ImmutableList.of());
        when(eventConsumer2.readEvents())
                .thenAnswer(invocation ->
                        ImmutableList.of(
                                new ConsumedEvent(
                                        null, NakadiCursor.of(timeline, "", ""), FAKE_EVENT_TIMESTAMP, null)));

        when(timelineService.createEventConsumer(anyString()))
                .thenReturn(eventConsumer1)
                .thenReturn(eventConsumer2);

        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);

        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");
        final NakadiCursor committedCursor2 = NakadiCursor.of(et1Timeline, "p2", "o2");

        final SubscriptionTimeLagService timeLagService = new SubscriptionTimeLagService(
                timelineService, new MetricRegistry(), nakadiSettings);
        final Map<EventTypePartition, Duration> timeLags =
                timeLagService.getTimeLags(ImmutableList.of(committedCursor1, committedCursor2));

        assertThat(timeLags.entrySet(), hasSize(2));

        // we don't control the order in which the consumers are used, but can assert that one lag is 0, and the other
        // one is not
        final Duration dur1 = timeLags.get(new EventTypePartition("et1", "p1"));
        final Duration dur2 = timeLags.get(new EventTypePartition("et1", "p2"));
        if (dur1.equals(Duration.ZERO)) {
            assertThat(dur2, greaterThan(Duration.ZERO));
        } else {
            assertThat(dur1, greaterThan(Duration.ZERO));
            assertThat(dur2, equalTo(Duration.ZERO));
        }
    }
}
