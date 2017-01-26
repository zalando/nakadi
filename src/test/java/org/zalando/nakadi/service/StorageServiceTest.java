package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageServiceTest {

    private StorageService storageService;
    private StorageDbRepository storageDbRepository;
    private TimelineService timelineService;
    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();

    @Before
    public void setUp() {
        storageDbRepository = mock(StorageDbRepository.class);
        timelineService = mock(TimelineService.class);
        storageService = new StorageService(objectMapper, storageDbRepository, timelineService);
    }

    @Test
    public void testCreateStorage() {
        final Storage dbReply = createTestStorage();

        when(storageDbRepository.createStorage(any())).thenReturn(dbReply);

        final JSONObject json = createTestStorageJson();
        final Result<Storage> result = storageService.createStorage(json);
        assertTrue(result.isSuccessful());
        final Storage storage = result.getValue();
        assertEquals(Storage.Type.KAFKA, storage.getType());
        assertEquals("https://localhost", storage.getKafkaConfiguration().getZkAddress());
        assertEquals("/path/to/kafka", storage.getKafkaConfiguration().getZkPath());
        assertNotNull(storage.getId());
    }

    @Test
    public void testCreateStorageMissingConfigItem() {
        final JSONObject json = createTestStorageJson();
        json.getJSONObject("configuration").remove("zk_path");
        final Result<Storage> result = storageService.createStorage(json);
        assertFalse(result.isSuccessful());
        System.out.println(result.getProblem().toString());
    }

    @Test
    public void testDeleteUnusedStorage() {
        final List<Timeline> timelines = createTimelines();
        final Storage storage = new Storage();
        storage.setId("s3");

        when(storageDbRepository.getStorage("s3")).thenReturn(Optional.of(storage));
        when(timelineService.listTimelines()).thenReturn(timelines);
        doNothing().when(storageDbRepository).deleteStorage("s3");
        assertTrue(storageService.deleteStorage("s3").isSuccessful());
    }

    @Test
    public void testDeleteStorageInUse() {
        final List<Timeline> timelines = createTimelines();

        final Storage storage = new Storage();
        storage.setId("s2");

        when(storageDbRepository.getStorage("s2")).thenReturn(Optional.of(storage));
        when(timelineService.listTimelines()).thenReturn(timelines);
        doNothing().when(storageDbRepository).deleteStorage("s2");

        assertFalse(storageService.deleteStorage("s2").isSuccessful());
    }

    @Test
    public void testDeleteNonExistingStorage() {
        final List<Timeline> timelines = createTimelines();

        when(storageDbRepository.getStorage("s4")).thenReturn(Optional.empty());
        when(timelineService.listTimelines()).thenReturn(timelines);
        doNothing().when(storageDbRepository).deleteStorage("s4");

        assertFalse(storageService.deleteStorage("s4").isSuccessful());
    }

    private JSONObject createTestStorageJson() {
        final JSONObject json = new JSONObject();
        json.put("storage_type", "kafka");
        final JSONObject configuration = new JSONObject();
        configuration.put("zk_address", "https://localhost");
        configuration.put("zk_path", "/path/to/kafka");
        json.put("configuration", configuration);
        return json;
    }

    private Storage createTestStorage() {
        final Storage storage = new Storage();
        storage.setType(Storage.Type.KAFKA);
        storage.setId("123-abc");
        final Storage.KafkaConfiguration configuration = new Storage.KafkaConfiguration();
        configuration.setZkPath("/path/to/kafka");
        configuration.setZkAddress("https://localhost");
        storage.setConfiguration(configuration);
        return storage;
    }

    private List<Timeline> createTimelines() {
        final Storage s1 = new Storage();
        s1.setId("s1");

        final Storage s2 = new Storage();
        s2.setId("s2");

        final List<Timeline> timelines = new ArrayList<>();

        final Timeline t1 = new Timeline("order_received", 0, s1, "topic", new Date());
        timelines.add(t1);

        final Timeline t2 = new Timeline("order_completed", 0, s1, "topic", new Date());
        timelines.add(t2);

        final Timeline t3 = new Timeline("order_received", 1, s2, "topic", new Date());
        timelines.add(t3);

        return timelines;
    }

    private List<Storage> createStorageList() {
        final List<Storage> storages = new ArrayList<>();

        final Storage s1 = new Storage();
        s1.setId("s1");
        storages.add(s1);

        final Storage s2 = new Storage();
        s2.setId("s2");
        storages.add(s2);

        final Storage s3 = new Storage();
        s3.setId("s3");
        storages.add(s3);

        return storages;
    }
}
