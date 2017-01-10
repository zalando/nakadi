package org.zalando.nakadi.repository.db;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.utils.TestUtils;

public class TimelineDbRepositoryTest extends AbstractDbRepositoryTest {

    private TimelineDbRepository tRepository;
    private StorageDbRepository sRepository;
    private EventTypeDbRepository eRepository;

    public TimelineDbRepositoryTest() {
        super("zn_data.timeline", "zn_data.storage", "zn_data.event_type_schema", "zn_data.event_type");
    }

    @Before
    public void setUp() {
        super.setUp();
        this.tRepository = new TimelineDbRepository(template, mapper);
        this.sRepository = new StorageDbRepository(template, mapper);
        this.eRepository = new EventTypeDbRepository(template, mapper);
    }

    @Test
    public void testTimelineCreated() throws InternalNakadiException, DuplicatedEventTypeNameException {
        final Storage storage = sRepository.createStorage(
                StorageDbRepositoryTest.createStorage("default", "test", "path"));
        final EventType testEt = eRepository.saveEventType(TestUtils.buildDefaultEventType());

        final Timeline timeline = createTimeline(
                storage, UUID.randomUUID(), 0, "test_topic", testEt.getName(),
                new Date(), null, null, null);

        tRepository.createTimeline(timeline);

        final Optional<Timeline> fromDb = tRepository.getTimeline(timeline.getId());
        Assert.assertTrue(fromDb.isPresent());
        Assert.assertFalse(fromDb.get() == timeline);
        Assert.assertEquals(timeline, fromDb.get());
    }

    @Test
    public void testTimelineUpdate() throws InternalNakadiException, DuplicatedEventTypeNameException {
        final Storage storage = sRepository.createStorage(
                StorageDbRepositoryTest.createStorage("default", "test", "path"));
        final EventType testEt = eRepository.saveEventType(TestUtils.buildDefaultEventType());

        final Timeline initial = tRepository.createTimeline(createTimeline(
                storage, UUID.randomUUID(), 0, "test_topic", testEt.getName(),
                new Date(), null, null, null));

        final Timeline modified = tRepository.getTimeline(initial.getId()).get();
        modified.setCreatedAt(new Date());
        modified.setCleanupAt(new Date());
        modified.setSwitchedAt(new Date());
        final Timeline.KafkaStoragePosition pos = new Timeline.KafkaStoragePosition();
        pos.setOffsets(LongStream.range(0L, 10L).mapToObj(Long::new).collect(Collectors.toList()));
        modified.setLatestPosition(pos);

        tRepository.updateTimelime(modified);
        final Timeline result = tRepository.getTimeline(modified.getId()).get();

        Assert.assertEquals(modified, result);
        Assert.assertNotEquals(initial, result);
    }

    @Test(expected = DuplicateKeyException.class)
    public void testDuplicateOrderNotAllowed() throws InternalNakadiException, DuplicatedEventTypeNameException {
        final Storage storage = sRepository.createStorage(
                StorageDbRepositoryTest.createStorage("default", "test", "path"));
        final EventType testEt = eRepository.saveEventType(TestUtils.buildDefaultEventType());

        tRepository.createTimeline(createTimeline(
                storage, UUID.randomUUID(), 0, "test_topic", testEt.getName(),
                new Date(), null, null, null));
        tRepository.createTimeline(createTimeline(
                storage, UUID.randomUUID(), 0, "test_topic", testEt.getName(),
                new Date(), null, null, null));
    }

    @Test
    public void testListTimelinesOrdered() throws InternalNakadiException, DuplicatedEventTypeNameException {
        final Storage storage = sRepository.createStorage(
                StorageDbRepositoryTest.createStorage("default", "test", "path"));
        final EventType testEt = eRepository.saveEventType(TestUtils.buildDefaultEventType());
        final Timeline t1 = tRepository.createTimeline(createTimeline(
                storage, UUID.randomUUID(), 1, "test_topic", testEt.getName(),
                new Date(), null, null, null));
        final Timeline t0 = tRepository.createTimeline(createTimeline(
                storage, UUID.randomUUID(), 0, "test_topic", testEt.getName(),
                new Date(), null, null, null));

        final List<Timeline> testTimelines = tRepository.listTimelines(testEt.getName());
        Assert.assertEquals(2, testTimelines.size());
        Assert.assertEquals(t0, testTimelines.get(0));
        Assert.assertEquals(t1, testTimelines.get(1));
    }

    @Test
    public void testTimelineDeleted() throws InternalNakadiException, DuplicatedEventTypeNameException {
        final Storage storage = sRepository.createStorage(
                StorageDbRepositoryTest.createStorage("default", "test", "path"));
        final EventType testEt = eRepository.saveEventType(TestUtils.buildDefaultEventType());
        final Timeline t1 = tRepository.createTimeline(createTimeline(
                storage, UUID.randomUUID(), 1, "test_topic", testEt.getName(),
                new Date(), null, null, null));
        Assert.assertEquals(1, tRepository.listTimelines(testEt.getName()).size());
        tRepository.deleteTimeline(t1.getId());
        Assert.assertEquals(0, tRepository.listTimelines(testEt.getName()).size());
    }

    private static Timeline createTimeline(
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
        timeline.setCleanupAt(cleanupAt);
        timeline.setLatestPosition(latestPosition);
        return timeline;
    }
}
