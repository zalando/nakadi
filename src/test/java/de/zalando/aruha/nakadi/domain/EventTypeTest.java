package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.JsonConfig;
import org.junit.Test;

import static de.zalando.aruha.nakadi.utils.TestUtils.resourceAsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class EventTypeTest {

    private final ObjectMapper objectMapper;

    public EventTypeTest() {
        objectMapper = new JsonConfig().jacksonObjectMapper();
    }

    @Test
    public void canDeserializeWithoutPartitioningKeyFields() throws Exception {
        final String json = resourceAsString("event-type.without.partitioning-key-fields.json", this.getClass());
        final EventType eventType = objectMapper.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }

    @Test
    public void canDeserializeWithPartitioningKeyFields() throws Exception {
        final String json = resourceAsString("event-type.with.partitioning-key-fields.json", this.getClass());
        final EventType eventType = objectMapper.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }


}