package org.zalando.nakadi.domain;

import org.junit.Test;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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

    @Test
    public void canDeserializeWithAnnotationsAndLabels() throws Exception {
        final String json = TestUtils.resourceAsString("event-type.with.annotations-and-labels.json", this.getClass());
        final EventType eventType = TestUtils.OBJECT_MAPPER.readValue(json, EventType.class);
        final Map<String, String> annotations = eventType.getAnnotations();
        final Map<String, String> labels = eventType.getLabels();
        assertThat(annotations, notNullValue());
        assertThat(labels, notNullValue());
        assertThat(annotations.size(), is(2));
        assertThat(labels.size(), is(2));
    }


}
