package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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

import java.util.Collections;
import java.util.List;

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
        final SubscriptionCursorWithoutToken beforeBegin0Converted = mock(SubscriptionCursorWithoutToken.class);
        when(cursorConverter.convertToNoToken(eq(beforeBegin0))).thenReturn(beforeBegin0Converted);
        final NakadiCursor beforeBegin1 = mock(NakadiCursor.class);
        final SubscriptionCursorWithoutToken beforeBegin1Converted = mock(SubscriptionCursorWithoutToken.class);
        when(cursorConverter.convertToNoToken(eq(beforeBegin1))).thenReturn(beforeBegin1Converted);

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

        final List<SubscriptionCursorWithoutToken> cursors = StartingState.calculateStartPosition(
                subscription, timelineService, cursorConverter);

        Assert.assertEquals(cursors.size(), 2);
        Assert.assertEquals(beforeBegin0Converted, cursors.get(0));
        Assert.assertEquals(beforeBegin1Converted, cursors.get(1));
    }

    @Test
    public void testGetSubscriptionOffsetsEnd() throws Exception {
        when(subscription.getReadFrom()).thenReturn(SubscriptionBase.InitialPosition.END);

        final NakadiCursor end0 = mock(NakadiCursor.class);
        final SubscriptionCursorWithoutToken end0Converted = mock(SubscriptionCursorWithoutToken.class);
        when(cursorConverter.convertToNoToken(eq(end0))).thenReturn(end0Converted);
        final NakadiCursor end1 = mock(NakadiCursor.class);
        final SubscriptionCursorWithoutToken end1Converted = mock(SubscriptionCursorWithoutToken.class);
        when(cursorConverter.convertToNoToken(eq(end1))).thenReturn(end1Converted);

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

        final List<SubscriptionCursorWithoutToken> cursors = StartingState.calculateStartPosition(
                subscription, timelineService, cursorConverter);
        Assert.assertEquals(cursors.size(), 2);
        Assert.assertEquals(end0Converted, cursors.get(0));
        Assert.assertEquals(end1Converted, cursors.get(1));
    }

    @Test
    public void testGetSubscriptionOffsetsCursors() throws Exception {
        when(subscription.getReadFrom()).thenReturn(SubscriptionBase.InitialPosition.CURSORS);
        final SubscriptionCursorWithoutToken cursor1 = mock(SubscriptionCursorWithoutToken.class);
        final SubscriptionCursorWithoutToken cursor2 = mock(SubscriptionCursorWithoutToken.class);
        when(subscription.getInitialCursors()).thenReturn(Lists.newArrayList(cursor1, cursor2));

        final List<SubscriptionCursorWithoutToken> cursors = StartingState.calculateStartPosition(
                subscription, timelineService, cursorConverter);
        Assert.assertEquals(cursors.size(), 2);
        Assert.assertEquals(cursor1, cursors.get(0));
        Assert.assertEquals(cursor2, cursors.get(1));
    }

}