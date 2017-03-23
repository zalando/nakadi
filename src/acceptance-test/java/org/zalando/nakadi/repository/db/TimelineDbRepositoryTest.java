package org.zalando.nakadi.repository.db;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.DuplicatedTimelineException;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TimelineDbRepositoryTest extends AbstractDbRepositoryTest {

    private TimelineDbRepository tRepository;
    private EventType testEt;
    private Storage storage;

    public TimelineDbRepositoryTest() {
        super("zn_data.timeline", "zn_data.storage", "zn_data.event_type_schema", "zn_data.event_type");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.tRepository = new TimelineDbRepository(template, mapper);
        final StorageDbRepository sRepository = new StorageDbRepository(template, mapper);
        final EventTypeDbRepository eRepository = new EventTypeDbRepository(template, mapper);

        storage = sRepository.createStorage(
                StorageDbRepositoryTest.createStorage("default", "localhost", 8181, "test", "path"));
        testEt = eRepository.saveEventType(TestUtils.buildDefaultEventType());
    }

    @Test
    public void testTimelineCreated() {

        final Timeline timeline = insertTimelineWithOrder(0);

        final Optional<Timeline> fromDb = tRepository.getTimeline(timeline.getId());
        Assert.assertTrue(fromDb.isPresent());
        Assert.assertFalse(fromDb.get() == timeline);
        Assert.assertEquals(timeline, fromDb.get());
    }

    @Test
    public void testTimelineUpdate() {
        final Timeline initial = insertTimelineWithOrder(0);

        final Timeline modified = tRepository.getTimeline(initial.getId()).get();
        modified.setCreatedAt(new Date());
        modified.setCleanedUpAt(new Date());
        modified.setSwitchedAt(new Date());
        final Timeline.KafkaStoragePosition pos = new Timeline.KafkaStoragePosition();
        pos.setOffsets(LongStream.range(0L, 10L).mapToObj(Long::new).collect(Collectors.toList()));
        modified.setLatestPosition(pos);

        tRepository.updateTimelime(modified);
        final Timeline result = tRepository.getTimeline(modified.getId()).get();

        Assert.assertEquals(modified, result);
        Assert.assertNotEquals(initial, result);
    }

    @Test(expected = DuplicatedTimelineException.class)
    public void testDuplicateOrderNotAllowed() {
        insertTimelineWithOrder(0);
        insertTimelineWithOrder(0);
    }

    @Test
    public void testListTimelinesOrdered() {
        final Timeline t1 = insertTimelineWithOrder(1);
        final Timeline t0 = insertTimelineWithOrder(0);

        final List<Timeline> testTimelines = tRepository.listTimelinesOrdered(testEt.getName());
        Assert.assertEquals(2, testTimelines.size());
        Assert.assertEquals(t0, testTimelines.get(0));
        Assert.assertEquals(t1, testTimelines.get(1));
    }

    @Test
    public void testTimelineDeleted() {
        final Timeline t1 = insertTimelineWithOrder(1);
        Assert.assertEquals(1, tRepository.listTimelinesOrdered(testEt.getName()).size());
        tRepository.deleteTimeline(t1.getId());
        Assert.assertEquals(0, tRepository.listTimelinesOrdered(testEt.getName()).size());
    }

    @Test
    public void testGetExpiredTimelines() {
        final DateTime now = new DateTime();
        final DateTime tomorrow = now.plusDays(1);
        final DateTime yesterday = now.minusDays(1);
        final DateTime twoDaysAgo = now.minusDays(2);

        insertTimeline(tomorrow.toDate(), true, 0);
        insertTimeline(tomorrow.toDate(), false, 1);
        insertTimeline(yesterday.toDate(), true, 2);
        final Timeline t4 = insertTimeline(yesterday.toDate(), false, 3);
        final Timeline t5 = insertTimeline(twoDaysAgo.toDate(), false, 4);
        insertTimeline(null, false, 5);

        final List<Timeline> expiredTimelines = tRepository.getExpiredTimelines();

        Assert.assertEquals(2, expiredTimelines.size());
        Assert.assertEquals(ImmutableList.of(t4, t5), expiredTimelines);
    }

    private Timeline insertTimelineWithOrder(final int order) {
        return tRepository.createTimeline(createTimeline(
                storage, UUID.randomUUID(), order, "test_topic", testEt.getName(),
                new Date(), null, null, null));
    }

    private Timeline insertTimeline(final Date cleanupDate, final boolean deleted, final int order) {
        final Timeline timeline = createTimeline(storage, UUID.randomUUID(), order, "test_topic", testEt.getName(),
                new Date(), new Date(), cleanupDate, null);
        timeline.setDeleted(deleted);
        return tRepository.createTimeline(timeline);
    }

    public static Timeline createTimeline(
            final Storage storage,
            final UUID id,
            final int order,
            final String topic,
            final String eventType,
            final Date createdAt,
            final Date switchedAt,
            final Date cleanupAt,
            final Timeline.StoragePosition latestPosition) {
        final Timeline timeline = new Timeline(eventType, order, storage, topic, createdAt);
        timeline.setId(id);
        timeline.setSwitchedAt(switchedAt);
        timeline.setCleanedUpAt(cleanupAt);
        timeline.setLatestPosition(latestPosition);
        return timeline;
    }
}
