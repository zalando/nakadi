package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.repository.db.StorageDbRepository;

import java.util.Optional;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageServiceTest {

    private StorageService storageService;
    private StorageDbRepository storageDbRepository;
    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();

    @Before
    public void setUp() {
        storageDbRepository = mock(StorageDbRepository.class);
        storageService = new StorageService(objectMapper, storageDbRepository);
    }

    @Test
    public void testCreateStorage() throws Exception {
        final Storage dbReply = createTestStorage();

        when(storageDbRepository.createStorage(any())).thenReturn(dbReply);

        final JSONObject storage = createTestStorageJson("s1");
        final Result<Void> result = storageService.createStorage(storage);
        assertTrue(result.isSuccessful());
    }

    @Test
    public void testDeleteUnusedStorage() throws Exception {
        final Storage storage = new Storage();
        storage.setId("s3");

        when(storageDbRepository.getStorage("s3")).thenReturn(Optional.of(storage));
        when(storageDbRepository.isStorageUsed("s3")).thenReturn(false);
        doNothing().when(storageDbRepository).deleteStorage("s3");
        assertTrue(storageService.deleteStorage("s3").isSuccessful());
    }

    @Test
    public void testDeleteStorageInUse() throws Exception {
        final Storage storage = new Storage();
        storage.setId("s2");

        when(storageDbRepository.getStorage("s2")).thenReturn(Optional.of(storage));
        when(storageDbRepository.isStorageUsed("s2")).thenReturn(true);
        doNothing().when(storageDbRepository).deleteStorage("s2");

        assertFalse(storageService.deleteStorage("s2").isSuccessful());
    }

    @Test
    public void testDeleteNonExistingStorage() throws Exception {
        when(storageDbRepository.getStorage("s4")).thenReturn(Optional.empty());
        when(storageDbRepository.isStorageUsed("s4")).thenReturn(false);
        doNothing().when(storageDbRepository).deleteStorage("s4");

        assertFalse(storageService.deleteStorage("s4").isSuccessful());
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
        final Storage.KafkaConfiguration configuration =
                new Storage.KafkaConfiguration("https://localhost", "/path/to/kafka");
        storage.setConfiguration(configuration);
        return storage;
    }
}
