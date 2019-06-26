package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.storage.DefaultStorage;
import org.zalando.nakadi.domain.storage.KafkaConfiguration;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageServiceTest {

    private StorageService storageService;
    private StorageDbRepository storageDbRepository;
    private FeatureToggleService featureToggleService;

    @Before
    public void setUp() {
        featureToggleService = mock(FeatureToggleService.class);
        storageDbRepository = mock(StorageDbRepository.class);
        final NakadiAuditLogPublisher auditLogPublisher = mock(NakadiAuditLogPublisher.class);
        storageService = new StorageService(TestUtils.OBJECT_MAPPER, storageDbRepository,
                new DefaultStorage(mock(Storage.class)), mock(ZooKeeperHolder.class), featureToggleService,
                auditLogPublisher);
        when(featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.DISABLE_DB_WRITE_OPERATIONS))
                .thenReturn(false);
    }

    @Test
    public void testCreateStorage() {
        final Storage dbReply = createTestStorage();

        when(storageDbRepository.createStorage(any())).thenReturn(dbReply);

        final JSONObject storage = createTestStorageJson("s1");
        storageService.createStorage(storage);
    }

    @Test
    public void testDeleteUnusedStorage() {
        when(storageDbRepository.getStorage(any())).thenReturn(Optional.empty());
        storageService.deleteStorage("s3");
    }

    @Test(expected = StorageIsUsedException.class)
    public void testDeleteStorageInUse() {
        when(storageDbRepository.getStorage(any())).thenReturn(Optional.empty());
        doThrow(new StorageIsUsedException("", null)).when(storageDbRepository).deleteStorage("s");

        storageService.deleteStorage("s");
    }

    @Test(expected = NoSuchStorageException.class)
    public void testDeleteNonExistingStorage() {
        when(storageDbRepository.getStorage(any())).thenReturn(Optional.empty());
        doThrow(new NoSuchStorageException("")).when(storageDbRepository).deleteStorage("s");

        storageService.deleteStorage("s");
    }

    private JSONObject createTestStorageJson(final String id) {
        final JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("storage_type", "kafka");
        final JSONObject configuration = new JSONObject();
        configuration.put("exhibitor_address", "https://localhost");
        configuration.put("exhibitor_port", 8181);
        configuration.put("zk_address", "https://localhost");
        configuration.put("zk_path", "/path/to/kafka");
        json.put("kafka_configuration", configuration);
        return json;
    }

    private Storage createTestStorage() {
        final Storage storage = new Storage();
        storage.setType(Storage.Type.KAFKA);
        storage.setId("123-abc");
        final KafkaConfiguration configuration =
                new KafkaConfiguration(ZookeeperConnection.valueOf("exhibitor://localhost:8181/path/to/kafka"));
        storage.setConfiguration(configuration);
        return storage;
    }
}
