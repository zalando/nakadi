package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.controller.advice.SettingsExceptionHandler;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.utils.TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER;

public class NakadiAvroSchemaControllerTest {
    private static final Logger LOG = LoggerFactory.getLogger(NakadiAvroSchemaControllerTest.class);
    private final SecuritySettings securitySettings = mock(SecuritySettings.class);
    private final AuthorizationService authorizationService = mock(AuthorizationService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private MockMvc mockMvc;

    @Before
    public void before() throws IOException {
        final LocalSchemaRegistry localSchemaRegistry = TestUtils.getLocalSchemaRegistry();
        final NakadiAvroSchemaController controller = new NakadiAvroSchemaController(
                localSchemaRegistry, new ObjectMapper());
        when(authorizationService.getSubject()).thenReturn(Optional.empty());

        this.mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(
                        securitySettings, authorizationService))
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new SettingsExceptionHandler())
                .build();
    }

    @Test
    public void testGetSchemaVersion() throws Exception {
        final var schemaName = LocalSchemaRegistry.BATCH_PUBLISHING_KEY;
        final var schemaVersion = "1";
        final var result = mockMvc.perform(
                        get("/avro-schemas/" + schemaName + "/versions/" + schemaVersion))
                .andExpect(status().isOk())
                .andReturn();
        final var response = result.getResponse().getContentAsString();
        LOG.info("Got response = {}", response);
        final var jsonResponse = objectMapper.readTree(response);
        assertTrue(jsonResponse.isObject());
        assertTrue(jsonResponse.get("version").isTextual());
        final var schema = jsonResponse.get("avro_schema");
        assertTrue(schema.isObject());
        assertEquals("record", schema.get("type").asText());
        assertEquals("PublishingBatch", schema.get("name").asText());
    }

    @Test
    public void testGetSchemas() throws Exception {
        final var schemaName = LocalSchemaRegistry.BATCH_PUBLISHING_KEY;
        final var result = mockMvc.perform(
                        get("/avro-schemas/" + schemaName + "/versions"))
                .andExpect(status().isOk())
                .andReturn();
        final var response = result.getResponse().getContentAsString();
        LOG.info("Got response = {}", response);
        final var jsonResponse = objectMapper.readTree(response);
        assertTrue(jsonResponse.isObject());
        final var items = jsonResponse.get("items");
        assertTrue(items.isArray());
        final var firstItem = items.get(0);
        assertTrue(firstItem.get("version").isTextual());
        assertTrue(firstItem.get("avro_schema").isObject());
    }

}