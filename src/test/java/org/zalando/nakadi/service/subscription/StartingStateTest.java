package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.subscription.state.StartingState;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartingStateTest {

    public static final String ET_0 = "et_0";
    public static final String ET_1 = "et_1";
    private Subscription subscription;
    private Timeline timelineEt00;
    private Timeline timelineEt01;
    private Timeline timelineEt10;
    private Timeline timelineEt11;
    private TimelineService timelineService;
    private CursorConverter cursorConverter;

    @Before
    public void setUp() throws Exception {
        this.subscription = mock(Subscription.class);
        when(subscription.getEventTypes()).thenReturn(ImmutableSet.of(ET_0, ET_1));
        this.timelineService = mock(TimelineService.class);
        timelineEt00 = mock(Timeline.class);
        timelineEt01 = mock(Timeline.class);
        timelineEt10 = mock(Timeline.class);
        timelineEt11 = mock(Timeline.class);
        when(timelineService.getActiveTimelinesOrdered(eq(ET_0)))
                .thenReturn(ImmutableList.of(timelineEt00, timelineEt01));
        when(timelineService.getActiveTimelinesOrdered(eq(ET_1)))
                .thenReturn(ImmutableList.of(timelineEt10, timelineEt11));

        this.cursorConverter = mock(CursorConverter.class);
    }

    @Test
    public void testGetSubscriptionOffsetsBegin() throws Exception {
        when(subscription.getReadFrom()).thenReturn(SubscriptionBase.InitialPosition.BEGIN);

        final NakadiCursor beforeBegin0 = mock(NakadiCursor.class);
        when(beforeBegin0.getTopic()).thenReturn(ET_0);
        when(beforeBegin0.getPartition()).thenReturn("0");
        final NakadiCursor beforeBegin1 = mock(NakadiCursor.class);
        when(beforeBegin1.getTopic()).thenReturn(ET_1);
        when(beforeBegin1.getPartition()).thenReturn("0");

        final TopicRepository firstTR = mock(TopicRepository.class);

        final List<PartitionStatistics> resultForTopic0 = Collections.singletonList(
                mock(PartitionStatistics.class));
        when(resultForTopic0.get(0).getBeforeFirst()).thenReturn(beforeBegin0);
        when(firstTR.loadTopicStatistics(eq(Collections.singletonList(timelineEt00))))
                .thenReturn(resultForTopic0);

        final TopicRepository secondTR = mock(TopicRepository.class);
        final List<PartitionStatistics> resultForTopic1 = Collections.singletonList(
                mock(PartitionStatistics.class));
        when(resultForTopic1.get(0).getBeforeFirst()).thenReturn(beforeBegin1);

        when(secondTR.loadTopicStatistics(eq(Collections.singletonList(timelineEt10))))
                .thenReturn(resultForTopic1);

        when(timelineService.getTopicRepository(eq(timelineEt00))).thenReturn(firstTR);
        when(timelineService.getTopicRepository(eq(timelineEt10))).thenReturn(secondTR);

        final List<NakadiCursor> cursors = StartingState.calculateStartPosition(
                subscription, timelineService, cursorConverter);

        Assert.assertEquals(cursors.size(), 2);
        Assert.assertEquals(beforeBegin0, cursors.get(0));
        Assert.assertEquals(beforeBegin1, cursors.get(1));
    }

    @Test
    public void testGetSubscriptionOffsetsEnd() throws Exception {
        when(subscription.getReadFrom()).thenReturn(SubscriptionBase.InitialPosition.END);

        final NakadiCursor end0 = mock(NakadiCursor.class);
        when(end0.getTopic()).thenReturn(ET_0);
        when(end0.getPartition()).thenReturn("0");
        final NakadiCursor end1 = mock(NakadiCursor.class);
        when(end1.getTopic()).thenReturn(ET_1);
        when(end1.getPartition()).thenReturn("0");

        final TopicRepository firstTR = mock(TopicRepository.class);
        final List<PartitionStatistics> statsForEt0 = Collections.singletonList(mock(PartitionStatistics.class));
        when(statsForEt0.get(0).getLast()).thenReturn(end0);
        when(firstTR.loadTopicStatistics(eq(Collections.singletonList(timelineEt01)))).thenReturn(statsForEt0);

        final TopicRepository secondTR = mock(TopicRepository.class);
        final List<PartitionStatistics> statsForTopic1 = Collections.singletonList(mock(PartitionStatistics.class));
        when(statsForTopic1.get(0).getLast()).thenReturn(end1);
        when(secondTR.loadTopicStatistics(eq(Collections.singletonList(timelineEt11)))).thenReturn(statsForTopic1);

        when(timelineService.getTopicRepository(eq(timelineEt01))).thenReturn(firstTR);
        when(timelineService.getTopicRepository(eq(timelineEt11))).thenReturn(secondTR);

        final List<NakadiCursor> cursors = StartingState.calculateStartPosition(subscription, timelineService,
                cursorConverter);
        Assert.assertEquals(cursors.size(), 2);
        Assert.assertEquals(end0, cursors.get(0));
        Assert.assertEquals(end1, cursors.get(1));
    }

    @Test
    public void testGetSubscriptionOffsetsCursors() throws Exception {
        when(subscription.getReadFrom()).thenReturn(SubscriptionBase.InitialPosition.CURSORS);
        final SubscriptionCursorWithoutToken cursor1 = mock(SubscriptionCursorWithoutToken.class);
        final SubscriptionCursorWithoutToken cursor2 = mock(SubscriptionCursorWithoutToken.class);
        when(subscription.getInitialCursors()).thenReturn(Lists.newArrayList(cursor1, cursor2));

        final NakadiCursor middle0 = mock(NakadiCursor.class);
        when(middle0.getTopic()).thenReturn(ET_0);
        when(middle0.getPartition()).thenReturn("0");

        final NakadiCursor middle1 = mock(NakadiCursor.class);
        when(middle1.getTopic()).thenReturn(ET_1);
        when(middle1.getPartition()).thenReturn("0");

        when(cursorConverter.convert(cursor1)).thenReturn(middle0);
        when(cursorConverter.convert(cursor2)).thenReturn(middle1);

        final List<NakadiCursor> cursors = StartingState.calculateStartPosition(subscription, timelineService,
                cursorConverter);
        Assert.assertEquals(cursors.size(), 2);
        Assert.assertEquals(middle0, cursors.get(0));
        Assert.assertEquals(middle1, cursors.get(1));
    }

}