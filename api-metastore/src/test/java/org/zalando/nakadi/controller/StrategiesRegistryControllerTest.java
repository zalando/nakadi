package org.zalando.nakadi.controller;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.zalando.nakadi.partitioning.PartitionResolver.ALL_PARTITION_STRATEGIES;
import static org.zalando.nakadi.utils.TestUtils.mockMvcForController;

public class StrategiesRegistryControllerTest {

    private static final MockMvc MOCK_MVC = mockMvcForController(new StrategiesRegistryController());

    @Test
    public void canExposePartitionStrategies() throws Exception {
        MOCK_MVC.perform(get("/registry/partition-strategies"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("$", Matchers.hasSize(ALL_PARTITION_STRATEGIES.size())));
    }

    @Test
    public void canExposeEnrichmentStrategies() throws Exception {
        MOCK_MVC.perform(get("/registry/enrichment-strategies"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().string("[\"metadata_enrichment\"]"));
    }

}
