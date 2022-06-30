package org.zalando.nakadi.service;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.storage.KafkaConfiguration;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StorageServiceTest {

    @Mock
    private StorageService storageService;
    @Mock
    private StorageDbRepository storageDbRepository;
    @Mock
    private TransactionTemplate transactionTemplate;
    @Mock
    private NakadiAuditLogPublisher auditLogPublisher;
    @Mock
    private FeatureToggleService featureToggleService;

    @Before
    public void setUp() {
        storageService = new StorageService(TestUtils.OBJECT_MAPPER, storageDbRepository, featureToggleService,
                auditLogPublisher, transactionTemplate, null);
        when(featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS))
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

    @Test
    public void whenMarkDefaultOthersAreUnmarked() {
        final List<Storage> storages = Arrays.asList(
                new Storage("id1", Storage.Type.KAFKA, true),
                new Storage("id2", Storage.Type.KAFKA, true),
                new Storage("id3", Storage.Type.KAFKA, false),
                new Storage("id4", Storage.Type.KAFKA, false)
        );
        when(storageDbRepository.listStorages()).thenReturn(storages);
        when(storageDbRepository.getStorage(eq("id4"))).thenReturn(Optional.of(storages.get(3)));
        when(transactionTemplate.execute(any())).thenAnswer(
                invocation -> ((TransactionCallback) invocation.getArgument(0)).doInTransaction(null));

        storageService.setDefaultStorage("id4");

        verify(storageDbRepository).setDefaultStorage(eq("id1"), eq(false));
        verify(storageDbRepository, never()).setDefaultStorage(eq("id1"), eq(true));

        verify(storageDbRepository).setDefaultStorage(eq("id2"), eq(false));
        verify(storageDbRepository, never()).setDefaultStorage(eq("id2"), eq(true));

        verify(storageDbRepository, never()).setDefaultStorage(eq("id3"), eq(true));
        verify(storageDbRepository, never()).setDefaultStorage(eq("id3"), eq(false));

        verify(storageDbRepository).setDefaultStorage(eq("id4"), eq(true));
        verify(storageDbRepository, never()).setDefaultStorage(eq("id4"), eq(false));
    }

    private JSONObject createTestStorageJson(final String id) {
        final JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("storage_type", "kafka");
        final JSONObject configuration = new JSONObject();
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
                new KafkaConfiguration(ZookeeperConnection.valueOf("zookeeper://localhost:8181/path/to/kafka"));
        storage.setConfiguration(configuration);
        return storage;
    }
}
