package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.security.auth.UserPrincipal;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.StorageService;
import org.zalando.nakadi.util.FeatureToggleService;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;


public class StoragesControllerTest {

    private final StorageService storageService = mock(StorageService.class);
    private final SecuritySettings securitySettings = mock(SecuritySettings.class);
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);

    private MockMvc mockMvc;

    @Before
    public void before() {
        final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();

        final StoragesController controller = new StoragesController(securitySettings, storageService);

        doReturn("nakadi").when(securitySettings).getAdminClientId();
        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .setCustomArgumentResolvers(new ClientResolver(securitySettings, featureToggleService))
                .build();
    }

    @Test
    public void testListStorages() throws Exception {
        final List<Storage> storages = createStorageList();
        when(storageService.listStorages())
                .thenReturn(storages);
        mockMvc.perform(get("/storages")
                .principal(new UserPrincipal("nakadi")))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteUnusedStorage() throws Exception {
        when(storageService.deleteStorage("s1"))
                .thenReturn(Result.ok());
        mockMvc.perform(delete("/storages/s1")
                .principal(new UserPrincipal("nakadi")))
                .andExpect(status().isOk());
    }

    @Test
    public void testDeleteStorageInUse() throws Exception {
        when(storageService.deleteStorage("s1"))
                .thenReturn(Result.forbidden("Storage in use"));
        mockMvc.perform(delete("/storages/s1")
                .principal(new UserPrincipal("nakadi")))
                .andExpect(status().isForbidden());
    }

    private List<Storage> createStorageList() {
        final List<Storage> storages = new ArrayList<>();

        final Storage s1 = new Storage();
        s1.setId("s1");
        s1.setType(Storage.Type.KAFKA);
        final Storage.KafkaConfiguration config = new Storage.KafkaConfiguration("http://localhost", "/path/to/kafka");
        s1.setConfiguration(config);
        storages.add(s1);

        final Storage s2 = new Storage();
        s2.setId("s2");
        s2.setType(Storage.Type.KAFKA);
        s2.setConfiguration(config);
        storages.add(s2);

        return storages;
    }
}
