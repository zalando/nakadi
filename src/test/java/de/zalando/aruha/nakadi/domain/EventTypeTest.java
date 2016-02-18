package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class EventTypeTest {

    private final ObjectMapper objectMapper;

    public EventTypeTest() {
        objectMapper = new NakadiConfig().jacksonObjectMapper();
    }

    @Test
    public void canDeserializeWithoutOrderingKeyFields() throws Exception {
        final String json = resourceAsString("event-type.without.ordering-key-fields.json");
        final EventType eventType = objectMapper.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }

    @Test
    public void canDeserializeWithOrderingKeyFields() throws Exception {
        final String json = resourceAsString("event-type.with.ordering-key-fields.json");
        final EventType eventType = objectMapper.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }

    private static String resourceAsString(final String resourceName) throws IOException {
        return IOUtils.toString(EventTypeTest.class.getResourceAsStream(resourceName));
    }



}