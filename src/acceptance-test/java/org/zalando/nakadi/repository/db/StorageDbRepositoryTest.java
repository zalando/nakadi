package org.zalando.nakadi.repository.db;

import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NoStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;

public class StorageDbRepositoryTest extends AbstractDbRepositoryTest {
    private StorageDbRepository repository;
    private TimelineDbRepository timelineDbRepository;
    private EventTypeDbRepository eventTypeDbRepository;

    public StorageDbRepositoryTest() {
        super("zn_data.timeline", "zn_data.event_type_schema", "zn_data.event_type", "zn_data.storage");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        repository = new StorageDbRepository(template, mapper);
        timelineDbRepository = new TimelineDbRepository(template, mapper);
        eventTypeDbRepository = new EventTypeDbRepository(template, mapper);
    }

    static Storage createStorage(final String name,
                                 final String exhibitorAddress,
                                 final int exhibitorPort,
                                 final String zkAddress,
                                 final String zkPath) {
        final Storage.KafkaConfiguration config =
                new Storage.KafkaConfiguration(exhibitorAddress, exhibitorPort, zkAddress, zkPath);
        final Storage storage = new Storage();
        storage.setId(name);
        storage.setType(Storage.Type.KAFKA);
        storage.setConfiguration(config);
        return storage;
    }

    @Test
    public void testStorageCreated() throws Exception {
        final Storage storage = createStorage("default", "exaddress", 8181, "address", "path");

        repository.createStorage(storage);

        final Optional<Storage> createdCopy = repository.getStorage(storage.getId());
        assertTrue(createdCopy.isPresent());
        assertNotSame(createdCopy.get(), storage);

        assertEquals(storage, createdCopy.get());
    }

    @Test
    public void testStorageOrdered() throws Exception {
        final Storage storage2 = repository.createStorage(createStorage("2", "exaddress", 8181, "address1", "path3"));
        final Storage storage1 = repository.createStorage(createStorage("1", "exaddress", 8181, "address2", "path2"));
        final Storage storage3 = repository.createStorage(createStorage("3", "exaddress", 8181, "address3", "path1"));

        final List<Storage> storages = repository.listStorages();
        assertEquals(3, storages.size());
        assertEquals(storage1, storages.get(0));
        assertEquals(storage2, storages.get(1));
        assertEquals(storage3, storages.get(2));
    }

    @Test
    public void testStorageDeleted() throws Exception {
        final Storage storage = repository.createStorage(createStorage("1", "exaddress", 8181, "address2", "path2"));
        assertEquals(storage, repository.getStorage(storage.getId()).get());
        repository.deleteStorage(storage.getId());
        assertFalse(repository.getStorage(storage.getId()).isPresent());
        assertEquals(0, repository.listStorages().size());
    }

    @Test(expected = NoStorageException.class)
    public void testDeleteNoneExistingStorage() throws Exception {
        repository.deleteStorage(randomUUID());
    }

    @Test(expected = StorageIsUsedException.class)
    public void testDeleteUsedStorage() throws Exception {
        final Storage storage = repository.createStorage(createStorage("1", "exaddress", 8181, "address", "path"));

        final EventType eventType = eventTypeDbRepository.saveEventType(TestUtils.buildDefaultEventType());
        final Timeline timeline = TimelineDbRepositoryTest.createTimeline(storage, UUID.randomUUID(), 0, "topic",
                eventType.getName(), new Date(), new Date(), null, null);
        timelineDbRepository.createTimeline(timeline);

        repository.deleteStorage(storage.getId());
    }

}
