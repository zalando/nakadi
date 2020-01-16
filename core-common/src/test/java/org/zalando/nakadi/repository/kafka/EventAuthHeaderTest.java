package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

public class EventAuthHeaderTest {

    private static final String HEADER_VALUE = "restricted_access_event";

    @Test
    public void testEventAuthHeaderSerialization() {
        final EventAuthHeader eventAuthHeader = new EventAuthHeader(HEADER_VALUE);
        Assert.assertEquals(HEADER_VALUE, eventAuthHeader.getEventAuthValue());

        // example
        final ProducerRecord<String, String> record = new ProducerRecord<>("key", "value");
        record.headers().add(eventAuthHeader);
        final EventAuthHeader eventAuthHeader2 =
                EventAuthHeader.valueOf(record.headers().lastHeader(EventAuthHeader.HEADER_KEY));
        Assert.assertEquals(HEADER_VALUE, eventAuthHeader2.getEventAuthValue());
    }

}