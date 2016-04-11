package de.zalando.aruha.nakadi.controller;

import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;

import static de.zalando.aruha.nakadi.partitioning.PartitionResolver.ALL_PARTITION_STRATEGIES;
import static de.zalando.aruha.nakadi.utils.TestUtils.mockMvcForController;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class StrategiesRegistryControllerTest {

    private static final MockMvc mockMvc = mockMvcForController(new StrategiesRegistryController());

    @Test
    public void canExposePartitionStrategies() throws Exception {
        mockMvc.perform(get("/registry/partition-strategies"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(ALL_PARTITION_STRATEGIES.size())));
    }

}
