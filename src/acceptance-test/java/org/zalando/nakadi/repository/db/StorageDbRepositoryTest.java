package org.zalando.nakadi.repository.db;

import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageDbRepositoryTest extends AbstractDbRepositoryTest {
    private StorageDbRepository repository;
    private TimelineDbRepository timelineDbRepository;
    private EventTypeDbRepository eventTypeDbRepository;

    public StorageDbRepositoryTest() {
        super("zn_data.timeline", "zn_data.event_type_schema", "zn_data.event_type", "zn_data.storage");
    }

    @Before
    public void setUp() {
        super.setUp();
        repository = new StorageDbRepository(template, mapper);
        timelineDbRepository = new TimelineDbRepository(template, mapper);
        eventTypeDbRepository = new EventTypeDbRepository(template, mapper);
    }

    static Storage createStorage(final String name, final String zkAddress, final String zkPath) {
        final Storage.KafkaConfiguration config = new Storage.KafkaConfiguration(zkAddress, zkPath);
        final Storage storage = new Storage();
        storage.setId(name);
        storage.setType(Storage.Type.KAFKA);
        storage.setConfiguration(config);
        return storage;
    }

    @Test
    public void testStorageCreated() throws Exception {
        final Storage storage = createStorage("default", "address", "path");

        repository.createStorage(storage);

        final Optional<Storage> createdCopy = repository.getStorage(storage.getId());
        assertTrue(createdCopy.isPresent());
        assertFalse(createdCopy.get() == storage);

        assertEquals(storage, createdCopy.get());
    }

    @Test
    public void testStorageOrdered() throws Exception {
        final Storage storage2 = repository.createStorage(createStorage("2", "address1", "path3"));
        final Storage storage1 = repository.createStorage(createStorage("1", "address2", "path2"));
        final Storage storage3 = repository.createStorage(createStorage("3", "address3", "path1"));

        final List<Storage> storages = repository.listStorages();
        assertEquals(3, storages.size());
        assertEquals(storage1, storages.get(0));
        assertEquals(storage2, storages.get(1));
        assertEquals(storage3, storages.get(2));
    }

    @Test
    public void testStorageDeleted() throws Exception {
        final Storage storage = repository.createStorage(createStorage("1", "address2", "path2"));
        assertEquals(storage, repository.getStorage(storage.getId()).get());
        repository.deleteStorage(storage.getId());
        assertFalse(repository.getStorage(storage.getId()).isPresent());
        assertEquals(0, repository.listStorages().size());
    }

    @Test
    public void testIsStorageUsedNo() throws Exception {
        final Storage storage1 = repository.createStorage(createStorage("s1", "address1", "path1"));
        assertFalse(repository.isStorageUsed("s1"));
    }

    @Test
    public void testIsStorageUsedYes() throws Exception {
        final Storage storage1 = repository.createStorage(createStorage("s2", "address1", "path1"));
        final EventType testEt = eventTypeDbRepository.saveEventType(TestUtils.buildDefaultEventType());
        final Timeline timeline = createTimeline(
                storage1, UUID.randomUUID(), 0, "test_topic", testEt.getName(),
                new Date(), null, null, null);
        timelineDbRepository.createTimeline(timeline);
        assertTrue(repository.isStorageUsed("s2"));
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
