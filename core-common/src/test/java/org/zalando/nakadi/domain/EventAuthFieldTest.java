package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

public class EventAuthFieldTest {

    private static final String NAME = "teams";
    public static final String VALUE = "nakadi";

    @Test
    public void testEventAuthHeaderSerialization() {
        final EventAuthField eventAuthField = new EventAuthField(NAME, VALUE);
        final ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");
        eventAuthField.serialize(record);

        Assert.assertEquals(NAME,
                new String(
                        record.headers().lastHeader(EventAuthField.AUTH_PARAM_NAME).value(),
                        Charsets.UTF_8));
        Assert.assertEquals(VALUE,
                new String(
                        record.headers().lastHeader(EventAuthField.AUTH_PARAM_VALUE).value(),
                        Charsets.UTF_8));
    }

    @Test
    public void testEventAuthHeaderDeserialization() {
        final ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>("topic", 1, 1L, "key".getBytes(), "value".getBytes());
        record.headers().add(EventAuthField.AUTH_PARAM_NAME, NAME.getBytes(Charsets.UTF_8));
        record.headers().add(EventAuthField.AUTH_PARAM_VALUE, VALUE.getBytes(Charsets.UTF_8));
        final EventAuthField eventAuthField = EventAuthField.deserialize(record);

        Assert.assertEquals(NAME, eventAuthField.getName());
        Assert.assertEquals(VALUE, eventAuthField.getValue());
    }
}
