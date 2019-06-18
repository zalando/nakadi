package org.zalando.nakadi.repository.db;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.exceptions.runtime.DuplicatedTimelineException;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.zalando.nakadi.utils.TestUtils.randomUUID;

public class TimelineDbRepositoryTest extends AbstractDbRepositoryTest {

    private TimelineDbRepository tRepository;
    private EventType testEt;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.tRepository = new TimelineDbRepository(template, TestUtils.OBJECT_MAPPER);
        final StorageDbRepository sRepository = new StorageDbRepository(template, TestUtils.OBJECT_MAPPER);
        final EventTypeDbRepository eRepository = new EventTypeDbRepository(template, TestUtils.OBJECT_MAPPER);

        storage = sRepository.createStorage(StorageDbRepositoryTest.createStorage(
                randomUUID(), ZookeeperConnection.valueOf("zookeeper://localhost:8181/test")));
        testEt = eRepository.saveEventType(TestUtils.buildDefaultEventType());
    }

    @Test
    public void testTimelineCreated() {

        final Timeline timeline = insertTimeline(0);

        final Optional<Timeline> fromDb = tRepository.getTimeline(timeline.getId());
        Assert.assertTrue(fromDb.isPresent());
        Assert.assertFalse(fromDb.get() == timeline);
        Assert.assertEquals(timeline, fromDb.get());
    }

    @Test
    public void testTimelineUpdate() {
        final Timeline initial = insertTimeline(0);

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
        insertTimeline(0);
        insertTimeline(0);
    }

    @Test
    public void testListTimelinesOrdered() {
        final Timeline t1 = insertTimeline(1);
        final Timeline t0 = insertTimeline(0);

        final List<Timeline> testTimelines = tRepository.listTimelinesOrdered(testEt.getName());
        Assert.assertEquals(2, testTimelines.size());
        Assert.assertEquals(t0, testTimelines.get(0));
        Assert.assertEquals(t1, testTimelines.get(1));
    }

    @Test
    public void testTimelineDeleted() {
        final Timeline t1 = insertTimeline(1);
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

        final Timeline t1 = insertTimeline(tomorrow.toDate(), true, 0);
        final Timeline t2 = insertTimeline(tomorrow.toDate(), false, 1);
        final Timeline t3 = insertTimeline(yesterday.toDate(), true, 2);
        final Timeline t4 = insertTimeline(yesterday.toDate(), false, 3);
        final Timeline t5 = insertTimeline(twoDaysAgo.toDate(), false, 4);
        final Timeline t6 = insertTimeline(null, false, 5);

        final List<Timeline> expiredTimelines = tRepository.getExpiredTimelines();

        Assert.assertFalse(expiredTimelines.contains(t1));
        Assert.assertFalse(expiredTimelines.contains(t2));
        Assert.assertFalse(expiredTimelines.contains(t3));
        Assert.assertTrue(expiredTimelines.contains(t4));
        Assert.assertTrue(expiredTimelines.contains(t5));
        Assert.assertFalse(expiredTimelines.contains(t6));
    }

    private Timeline insertTimeline(final int order) {
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
