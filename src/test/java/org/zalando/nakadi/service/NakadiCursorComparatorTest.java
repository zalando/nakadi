package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.kafka.KafkaCursor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.LongStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NakadiCursorComparatorTest {

    private static final String ET = "et";
    private static final int TIMELINE_ET_COUNT = 10;

    private static Timeline[] TIMELINES = new Timeline[TIMELINE_ET_COUNT];
    private static EventTypeCache etCache;


    @BeforeClass
    public static void setupMocks() throws InternalNakadiException, NoSuchEventTypeException {
        etCache = mock(EventTypeCache.class);
        final List<Timeline> timelines = new ArrayList<>();
        final Storage storage = new Storage("default", Storage.Type.KAFKA);
        // [1 1 1] -> [] -> [] -> [1 1 1] -> [] -> [] -> [1 1 1] -> [] -> [] -> [1 1 1]
        for (int i = 0; i < TIMELINE_ET_COUNT; ++i) {
            final Timeline t = new Timeline(ET, 1 + i, storage, UUID.randomUUID().toString(), new Date());
            if (t.getOrder() != TIMELINE_ET_COUNT) {
                Timeline.KafkaStoragePosition latest = new Timeline.KafkaStoragePosition(
                        Collections.singletonList(i % 3 == 0 ? 2L : -1L));
                t.setLatestPosition(latest);
            }
            TIMELINES[i] = t;
        }
        when(etCache.getTimelinesOrdered(ET)).thenReturn(timelines);
    }

    @Test
    public void test() {
        final Comparator<NakadiCursor> comparator = new NakadiCursorComparator(etCache);

        final List<NakadiCursor> cursors = new ArrayList<>();
        for (final Timeline t: TIMELINES) {
            final long countToAdd = null == t.getLatestPosition() ? 5 :
                    ((Timeline.KafkaStoragePosition)t.getLatestPosition()).getLastOffsetForPartition(0) + 1;
            LongStream.range(0, countToAdd)
                    .mapToObj(i -> new NakadiCursor(t, "0", KafkaCursor.toNakadiOffset(i)))
                    .forEach(cursors::add);
        }

        for (int i = 0; i < cursors.size(); ++i) {
            final NakadiCursor first = cursors.get(i);
            for (int j = 0; j < cursors.size(); ++j) {
                final NakadiCursor second = cursors.get(j);

                final int expected = Integer.compare(i, j);
                final int real = comparator.compare(first, second);

                Assert.assertTrue(
                        (expected > 0 && real > 0) ||
                        (expected < 0 && real < 0) ||
                        (expected == 0 && real == 0)
                );
            }
        }

    }

}