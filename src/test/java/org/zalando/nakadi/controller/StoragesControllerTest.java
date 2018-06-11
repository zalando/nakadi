package org.zalando.nakadi.controller;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidStorageConfigurationException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.StorageService;
import org.zalando.nakadi.utils.TestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.util.PrincipalMockFactory.mockPrincipal;


public class StoragesControllerTest {

    private final StorageService storageService = mock(StorageService.class);
    private final SecuritySettings securitySettings = mock(SecuritySettings.class);
    private final AdminService adminService = mock(AdminService.class);
    private MockMvc mockMvc;

    @Before
    public void before() {
        final StoragesController controller = new StoragesController(storageService, adminService);
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
                .thenReturn(storages);
        when(adminService.isAdmin(AuthorizationService.Operation.READ)).thenReturn(true);
        mockMvc.perform(get("/storages")
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteUnusedStorage() throws Exception {
        doNothing().when(storageService).deleteStorage("s1");
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);

        mockMvc.perform(delete("/storages/s1")
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isNoContent());
    }

    @Test
    public void testDeleteStorageInUse() throws Exception {
        doThrow(new StorageIsUsedException("Storage in use", new Exception()))
                .when(storageService).deleteStorage("s1");
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);

        mockMvc.perform(delete("/storages/s1")
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isForbidden());
    }

    @Test
    public void testPostStorage() throws Exception {
        final JSONObject json = createJsonKafkaStorage("test_storage");
        doNothing().when(storageService).createStorage(any());
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);

        mockMvc.perform(post("/storages")
                .contentType(APPLICATION_JSON)
                .content(json.toString())
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isCreated());
    }

    @Test
    public void testPostStorageWithExistingId() throws Exception {
        final JSONObject json = createJsonKafkaStorage("test_storage");
        doThrow(new DuplicatedStorageException("Duplicated storage", new Exception()))
                .when(storageService).createStorage(any());
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);

        mockMvc.perform(post("/storages")
                .contentType(APPLICATION_JSON)
                .content(json.toString())
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isConflict());
    }

    @Test
    public void testPostStorageWrongFormat() throws Exception {
        final JSONObject json = createJsonKafkaStorage("test_storage");
        doThrow(new InvalidStorageConfigurationException("Invalid configuration"))
                .when(storageService).createStorage(any());
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);

        mockMvc.perform(post("/storages")
                .contentType(APPLICATION_JSON)
                .content(json.toString())
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    public void testSetDefaultStorageOk() throws Exception {
        when(storageService.setDefaultStorage("test_storage"))
                .thenReturn(createKafkaStorage("test_storage"));
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);
        mockMvc.perform(put("/storages/default/test_storage")
                .contentType(APPLICATION_JSON)
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isOk());
    }

    @Test
    public void testSetDefaultStorageNotFound() throws Exception {
        when(storageService.setDefaultStorage("test_storage"))
                .thenThrow(new NoSuchStorageException("No storage with id test_storage"));
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);
        mockMvc.perform(put("/storages/default/test_storage")
                .contentType(APPLICATION_JSON)
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isNotFound());
    }

    @Test
    public void testSetDefaultStorageInternalServiceError() throws Exception {
        when(storageService.setDefaultStorage("test_storage"))
                .thenThrow(new InternalNakadiException("Error while setting default storage in zk"));
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);
        mockMvc.perform(put("/storages/default/test_storage")
                .contentType(APPLICATION_JSON)
                .principal(mockPrincipal("nakadi")))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testSetDefaultStorageAccessDenied() throws Exception {
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(false);
        mockMvc.perform(put("/storages/default/test_storage")
                .contentType(APPLICATION_JSON)
                .principal(mockPrincipal("nakadi")))
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
