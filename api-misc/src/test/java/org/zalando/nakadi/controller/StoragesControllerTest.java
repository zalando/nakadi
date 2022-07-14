package org.zalando.nakadi.controller;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.controller.advice.SettingsExceptionHandler;
import org.zalando.nakadi.domain.storage.KafkaConfiguration;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.StorageService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.utils.TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER;


public class StoragesControllerTest {

    private final StorageService storageService = mock(StorageService.class);
    private final SecuritySettings securitySettings = mock(SecuritySettings.class);
    private final AdminService adminService = mock(AdminService.class);
    private final AuthorizationService authorizationService = mock(AuthorizationService.class);
    private MockMvc mockMvc;

    @Before
    public void before() {
        final StoragesController controller = new StoragesController(storageService, adminService);

        when(authorizationService.getSubject()).thenReturn(Optional.empty());
        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(
                        securitySettings, authorizationService))
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new SettingsExceptionHandler())
                .build();
    }

    @Test
    public void testListStorages() throws Exception {
        final List<Storage> storages = createStorageList();
        when(storageService.listStorages())
                .thenReturn(storages);
        when(adminService.isAdmin(AuthorizationService.Operation.READ)).thenReturn(true);
        mockMvc.perform(get("/storages"))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteUnusedStorage() throws Exception {
        doNothing().when(storageService).deleteStorage("s1");
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);
        mockMvc.perform(delete("/storages/s1"))
                .andExpect(status().isNoContent());
    }

    @Test
    public void testPostStorage() throws Exception {
        final JSONObject json = createJsonKafkaStorage("test_storage");
        doNothing().when(storageService).createStorage(any());
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);
        mockMvc.perform(post("/storages")
                        .contentType(APPLICATION_JSON)
                        .content(json.toString()))
                .andExpect(status().isCreated());
    }

    @Test
    public void testSetDefaultStorageOk() throws Exception {
        when(storageService.setDefaultStorage("test_storage"))
                .thenReturn(createKafkaStorage("test_storage"));
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);
        mockMvc.perform(put("/storages/default/test_storage")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testSetDefaultStorageAccessDenied() throws Exception {
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(false);
        mockMvc.perform(put("/storages/default/test_storage")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isForbidden());
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
        final KafkaConfiguration config = new KafkaConfiguration(
                ZookeeperConnection.valueOf("zookeeper://localhost:8181/path/to/kafka"));
        storage.setConfiguration(config);

        return storage;
    }

    private JSONObject createJsonKafkaStorage(final String id) {
        final JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("storage_type", "kafka");
        final JSONObject config = new JSONObject();
        config.put("zoookeeper_connection", "zookeeper://localhost:8181/path/to/kafka");
        json.put("kafka_configuration", config);

        return json;
    }
}
