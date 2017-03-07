package org.zalando.nakadi.service.converter;

import java.util.Collections;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CursorConverterImplTest {

    @Test
    public void testGuessVersionForObsoleteOffsets() {
        Assert.assertEquals(NakadiCursor.Version.ZERO, CursorConverterImpl.guessVersion("0"));
    }

    @Test
    public void testGuessVersionForIncorrectOffsets() {
        Assert.assertEquals(NakadiCursor.Version.ZERO, CursorConverterImpl.guessVersion("0101000001"));
    }

    @Test
    public void testGuessVersionZero() {
        Assert.assertEquals(NakadiCursor.Version.ZERO, CursorConverterImpl.guessVersion("000000000000000"));
    }

    @Test
    public void testGuessVersionOne() {
        Assert.assertEquals(NakadiCursor.Version.ONE, CursorConverterImpl.guessVersion("001"));
        Assert.assertEquals(NakadiCursor.Version.ONE, CursorConverterImpl.guessVersion("001-0000-0000000000000000"));
    }

    @Test
    public void testBeginConvertedVersionZero() throws Exception {
        final String eventType = "test-et";
        final String partition = "2";
        final Timeline timeline = mock(Timeline.class);
        final EventTypeCache eventTypeCache = mock(EventTypeCache.class);

        final TopicRepository topicRepository = mock(TopicRepository.class);
        final TimelineService timelineService = mock(TimelineService.class);
        final PartitionStatistics stats = mock(PartitionStatistics.class);
        when(timelineService.getActiveTimelinesOrdered(eq(eventType))).thenReturn(Collections.singletonList(timeline));
        when(timelineService.getTopicRepository(eq(timeline))).thenReturn(topicRepository);
        when(topicRepository.loadPartitionStatistics(eq(timeline), eq(partition))).thenReturn(Optional.of(stats));
        when(stats.getBeforeFirst()).thenReturn(new NakadiCursor(timeline, partition, "000001"));

        final CursorConverter converter = new CursorConverterImpl(eventTypeCache, timelineService);

        final NakadiCursor nakadiCursor = converter.convert(eventType, new Cursor(partition, "BEGIN"));
        Assert.assertEquals(timeline, nakadiCursor.getTimeline());
        Assert.assertEquals(partition, nakadiCursor.getPartition());
        Assert.assertEquals("000001", nakadiCursor.getOffset());
    }
}