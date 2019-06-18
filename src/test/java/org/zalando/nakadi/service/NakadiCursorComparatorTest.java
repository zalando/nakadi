package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.kafka.KafkaCursor;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NakadiCursorComparatorTest {

    private static final String ET = "et";

    private static final Storage STORAGE = new Storage("default", Storage.Type.KAFKA);
    private static final Timeline TIMELINE_1 = new Timeline(ET, 1, STORAGE, UUID.randomUUID().toString(), new Date());
    private static final Timeline TIMELINE_2 = new Timeline(ET, 2, STORAGE, UUID.randomUUID().toString(), new Date());
    private static final Timeline TIMELINE_3 = new Timeline(ET, 3, STORAGE, UUID.randomUUID().toString(), new Date());
    private static final Timeline TIMELINE_4 = new Timeline(ET, 4, STORAGE, UUID.randomUUID().toString(), new Date());

    private static Comparator<NakadiCursor> comparator;

    @BeforeClass
    public static void setupMocks() throws InternalNakadiException, NoSuchEventTypeException {
        final EventTypeCache etCache = mock(EventTypeCache.class);
        TIMELINE_1.setLatestPosition(new Timeline.KafkaStoragePosition(Collections.singletonList(1L)));
        TIMELINE_2.setLatestPosition(new Timeline.KafkaStoragePosition(Collections.singletonList(-1L)));
        TIMELINE_3.setLatestPosition(new Timeline.KafkaStoragePosition(Collections.singletonList(2L)));
        when(etCache.getTimelinesOrdered(ET)).thenReturn(Arrays.asList(TIMELINE_1, TIMELINE_2, TIMELINE_3, TIMELINE_4));
        comparator = new NakadiCursorComparator(etCache);
    }

    @Test
    public void testComparisionWithinTimeline() {
        checkGreater(cursor(TIMELINE_1, -1), cursor(TIMELINE_1, 0));
        checkGreater(cursor(TIMELINE_1, 0), cursor(TIMELINE_1, 1));
    }

    @Test
    public void testEqualityInCornerCases() {
        checkEqual(cursor(TIMELINE_1, 1), cursor(TIMELINE_2, -1));
        checkEqual(cursor(TIMELINE_1, 1), cursor(TIMELINE_3, -1));
    }

    @Test
    public void testInDifferentTimelines() {
        checkGreater(cursor(TIMELINE_1, 0), cursor(TIMELINE_3, 0));
        checkGreater(cursor(TIMELINE_2, -1), cursor(TIMELINE_3, 0));
    }

    private static void checkGreater(final NakadiCursor less, final NakadiCursor greater) {
        Assert.assertTrue("Cursor " + less + " should be less than " + greater,
                comparator.compare(less, greater) < 0);
        Assert.assertTrue("Cursor " + greater + " should be greater than " + less,
                comparator.compare(greater, less) > 0);
    }

    private static void checkEqual(final NakadiCursor first, final NakadiCursor second) {
        Assert.assertEquals("Cursor " + first + " should be equal to " + second,
                0,
                comparator.compare(first, second));
        Assert.assertEquals("Cursor " + second + " should be equal to " + first,
                0,
                comparator.compare(second, first));
    }

    private static NakadiCursor cursor(final Timeline timeline, final long offset) {
        return NakadiCursor.of(timeline, KafkaCursor.toNakadiPartition(0), KafkaCursor.toNakadiOffset(offset));
    }
}