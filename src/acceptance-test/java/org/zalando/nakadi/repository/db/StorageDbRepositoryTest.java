package org.zalando.nakadi.repository.db;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.Storage;

import java.util.List;
import java.util.Optional;

public class StorageDbRepositoryTest extends AbstractDbRepositoryTest {
    private StorageDbRepository repository;

    public StorageDbRepositoryTest() {
        super("zn_data.storage");
    }

    @Before
    public void setUp() {
        super.setUp();
        repository = new StorageDbRepository(template, mapper);
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
    public void testStorageCreated() {
        final Storage storage = createStorage("default", "address", "path");

        repository.createStorage(storage);

        final Optional<Storage> createdCopy = repository.getStorage(storage.getId());
        Assert.assertTrue(createdCopy.isPresent());
        Assert.assertFalse(createdCopy.get() == storage);

        Assert.assertEquals(storage, createdCopy.get());
    }

    @Test
    public void testStorageOrdered() {
        final Storage storage2 = repository.createStorage(createStorage("2", "address1", "path3"));
        final Storage storage1 = repository.createStorage(createStorage("1", "address2", "path2"));
        final Storage storage3 = repository.createStorage(createStorage("3", "address3", "path1"));

        final List<Storage> storages = repository.listStorages();
        Assert.assertEquals(3, storages.size());
        Assert.assertEquals(storage1, storages.get(0));
        Assert.assertEquals(storage2, storages.get(1));
        Assert.assertEquals(storage3, storages.get(2));
    }

    @Test
    public void testStorageDeleted() {
        final Storage storage = repository.createStorage(createStorage("1", "address2", "path2"));
        Assert.assertEquals(storage, repository.getStorage(storage.getId()).get());
        repository.deleteStorage(storage.getId());
        Assert.assertFalse(repository.getStorage(storage.getId()).isPresent());
        Assert.assertEquals(0, repository.listStorages().size());
    }
}
