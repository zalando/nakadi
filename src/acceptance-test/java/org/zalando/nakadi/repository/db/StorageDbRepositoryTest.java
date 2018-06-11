package org.zalando.nakadi.repository.db;

import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;
import static org.zalando.nakadi.utils.TestUtils.randomValidStringOfLength;

public class StorageDbRepositoryTest extends AbstractDbRepositoryTest {
    private StorageDbRepository repository;
    private TimelineDbRepository timelineDbRepository;
    private EventTypeDbRepository eventTypeDbRepository;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        repository = new StorageDbRepository(template, TestUtils.OBJECT_MAPPER);
        timelineDbRepository = new TimelineDbRepository(template, TestUtils.OBJECT_MAPPER);
        eventTypeDbRepository = new EventTypeDbRepository(template, TestUtils.OBJECT_MAPPER);
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
        final String name = randomUUID();
        final Storage storage = createStorage(name, "exaddress", 8181, "address", "path");

        repository.createStorage(storage);

        final Optional<Storage> createdCopy = repository.getStorage(storage.getId());
        assertTrue(createdCopy.isPresent());
        assertNotSame(createdCopy.get(), storage);

        assertEquals(storage, createdCopy.get());
    }

    @Test
    public void testStorageOrdered() throws Exception {
        final String namePrefix = randomValidStringOfLength(31);

        final Storage storage2 = repository.createStorage(
                createStorage(namePrefix + "2", "exaddress", 8181, "address1", "path3"));
        final Storage storage1 = repository.createStorage(
                createStorage(namePrefix + "1", "exaddress", 8181, "address2", "path2"));
        final Storage storage3 = repository.createStorage(
                createStorage(namePrefix + "3", "exaddress", 8181, "address3", "path1"));

        final List<Storage> storages = repository.listStorages().stream()
                .filter(st -> st.getId() != null)
                .filter(st -> st.getId().startsWith(namePrefix))
                .collect(Collectors.toList());

        assertEquals(3, storages.size());
        assertEquals(storage1, storages.get(0));
        assertEquals(storage2, storages.get(1));
        assertEquals(storage3, storages.get(2));
    }

    @Test
    public void testStorageDeleted() throws Exception {
        final String name = randomUUID();
        final Storage storage = repository.createStorage(createStorage(name, "exaddress", 8181, "address2", "path2"));
        assertEquals(storage, repository.getStorage(storage.getId()).get());
        repository.deleteStorage(storage.getId());
        assertFalse(repository.getStorage(storage.getId()).isPresent());
    }

    @Test(expected = NoSuchStorageException.class)
    public void testDeleteNonExistingStorage() throws Exception {
        repository.deleteStorage(randomUUID());
    }

    @Test(expected = StorageIsUsedException.class)
    public void testDeleteUsedStorage() throws Exception {
        final String name = randomUUID();
        final Storage storage = repository.createStorage(createStorage(name, "exaddress", 8181, "address", "path"));

        final EventType eventType = eventTypeDbRepository.saveEventType(TestUtils.buildDefaultEventType());
        final Timeline timeline = TimelineDbRepositoryTest.createTimeline(storage, UUID.randomUUID(), 0, "topic",
                eventType.getName(), new Date(), new Date(), null, null);
        timelineDbRepository.createTimeline(timeline);

        repository.deleteStorage(storage.getId());
    }

}
