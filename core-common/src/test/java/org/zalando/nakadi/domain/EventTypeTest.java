package org.zalando.nakadi.domain;

import org.junit.Test;
import org.zalando.nakadi.utils.TestUtils;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class EventTypeTest {

    @Test
    public void canDeserializeWithoutPartitionKeyFields() throws Exception {
        final String json = TestUtils.resourceAsString("event-type.without.partition-key-fields.json", this.getClass());
        final EventType eventType = TestUtils.OBJECT_MAPPER.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }

    @Test
    public void canDeserializeWithPartitionKeyFields() throws Exception {
        final String json = TestUtils.resourceAsString("event-type.with.partition-key-fields.json", this.getClass());
        final EventType eventType = TestUtils.OBJECT_MAPPER.readValue(json, EventType.class);

        assertThat(eventType, notNullValue());
    }


}
