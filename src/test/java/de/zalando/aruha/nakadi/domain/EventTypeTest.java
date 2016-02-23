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
    public void canDeserializeWithoutOrderingKeyFields() throws Exception {
        final String json = resourceAsString("event-type.without.ordering-key-fields.json", this.getClass());
        final EventType eventType = objectMapper.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }

    @Test
    public void canDeserializeWithOrderingKeyFields() throws Exception {
        final String json = resourceAsString("event-type.with.ordering-key-fields.json", this.getClass());
        final EventType eventType = objectMapper.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }


}