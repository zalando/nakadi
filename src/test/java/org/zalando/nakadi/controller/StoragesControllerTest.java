package org.zalando.nakadi.controller;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.StorageService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.problem.Problem;

import java.util.ArrayList;
import java.util.List;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.util.PrincipalMockFactory.mockPrincipal;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;


public class StoragesControllerTest {

    private final StorageService storageService = mock(StorageService.class);
    private final SecuritySettings securitySettings = mock(SecuritySettings.class);
    private MockMvc mockMvc;

    @Before
    public void before() {
        final StoragesController controller = new StoragesController(securitySettings, storageService);
        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);

        doReturn("nakadi").when(securitySettings).getAdminClientId();
        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(securitySettings, featureToggleService))
                .build();
    }

    @Test
    public void testListStorages() throws Exception {
        final List<Storage> storages = createStorageList();
        when(storageService.listStorages())
                .thenReturn(Result.ok(storages));
        mockMvc.perform(get("/storages")
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteUnusedStorage() throws Exception {
        when(storageService.deleteStorage("s1"))
                .thenReturn(Result.ok());
        mockMvc.perform(delete("/storages/s1")
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isNoContent());
    }

    @Test
    public void testDeleteStorageInUse() throws Exception {
        when(storageService.deleteStorage("s1"))
                .thenReturn(Result.forbidden("Storage in use"));
        mockMvc.perform(delete("/storages/s1")
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isForbidden());
    }

    @Test
    public void testPostStorage() throws Exception {
        final JSONObject json = createJsonKafkaStorage("test_storage");
        when(storageService.createStorage(any())).thenReturn(Result.ok());
        mockMvc.perform(post("/storages")
                .contentType(APPLICATION_JSON)
                .content(json.toString())
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isCreated());
    }

    @Test
    public void testPostStorageWithExistingId() throws Exception {
        final JSONObject json = createJsonKafkaStorage("test_storage");
        when(storageService.createStorage(any())).thenReturn(Result.problem(Problem.valueOf(CONFLICT)));
        mockMvc.perform(post("/storages")
                .contentType(APPLICATION_JSON)
                .content(json.toString())
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isConflict());
    }

    @Test
    public void testPostStorageWrongFormat() throws Exception {
        final JSONObject json = createJsonKafkaStorage("test_storage");
        when(storageService.createStorage(any())).thenReturn(Result.problem(Problem.valueOf(UNPROCESSABLE_ENTITY)));
        mockMvc.perform(post("/storages")
                .contentType(APPLICATION_JSON)
                .content(json.toString())
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isUnprocessableEntity());
    }

    private List<Storage> createStorageList() {
        final List<Storage> storages = new ArrayList<>();

        storages.add(createKafkaStorage("s1"));
        storages.add(createKafkaStorage("s2"));

        return storages;
    }

    public static Storage createKafkaStorage(final String id) {
        final Storage storage = new Storage();
        storage.setId(id);
        storage.setType(Storage.Type.KAFKA);
        final Storage.KafkaConfiguration config =
                new Storage.KafkaConfiguration("https://localhost", 8181, "https://localhost", "/path/to/kafka");
        storage.setConfiguration(config);

        return storage;
    }

    private JSONObject createJsonKafkaStorage(final String id) {
        final JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("storage_type", "kafka");
        final JSONObject config = new JSONObject();
        config.put("zk_address", "http://localhost");
        config.put("zk_path", "/path/to/kafka");
        json.put("kafka_configuration", config);

        return json;
    }
}
