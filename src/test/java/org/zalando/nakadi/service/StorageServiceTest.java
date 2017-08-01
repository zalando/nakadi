package org.zalando.nakadi.service;

import static org.hamcrest.CoreMatchers.equalTo;
import org.json.JSONObject;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.runtime.NoStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.utils.TestUtils;

public class StorageServiceTest {

    private StorageService storageService;
    private StorageDbRepository storageDbRepository;

    @Before
    public void setUp() {
        storageDbRepository = mock(StorageDbRepository.class);
        storageService = new StorageService(TestUtils.OBJECT_MAPPER, storageDbRepository);
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
        assertTrue(storageService.deleteStorage("s3").isSuccessful());
    }

    @Test
    public void testDeleteStorageInUse() throws Exception {
        doThrow(new StorageIsUsedException("", null)).when(storageDbRepository).deleteStorage("s");

        final Result<Void> result = storageService.deleteStorage("s");

        final Result<Void> expectedResult = Result.forbidden("Storage s is in use");
        assertThat(result.getProblem().getStatus(), equalTo(expectedResult.getProblem().getStatus()));
        assertThat(result.getProblem().getDetail(), equalTo(expectedResult.getProblem().getDetail()));
    }

    @Test
    public void testDeleteNonExistingStorage() throws Exception {
        doThrow(new NoStorageException("")).when(storageDbRepository).deleteStorage("s");

        final Result<Void> result = storageService.deleteStorage("s");

        final Result<Void> expectedResult = Result.notFound("No storage with ID s");
        assertThat(result.getProblem().getStatus(), equalTo(expectedResult.getProblem().getStatus()));
        assertThat(result.getProblem().getDetail(), equalTo(expectedResult.getProblem().getDetail()));
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
        final Storage.KafkaConfiguration configuration =
                new Storage.KafkaConfiguration("https://localhost", 8181, "https://localhost", "/path/to/kafka");
        storage.setConfiguration(configuration);
        return storage;
    }
}
