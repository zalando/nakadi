package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class EventOwnerHeaderTest {

    private static final String NAME = "teams";
    public static final String VALUE = "nakadi";

    @Test
    public void testEventOwnerHeaderSerialization() {
        final EventOwnerHeader eventOwnerHeader = new EventOwnerHeader(NAME, VALUE);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                "topic",
                "value".getBytes(StandardCharsets.UTF_8));
        eventOwnerHeader.serialize(record);

        Assert.assertEquals(NAME,
                new String(
                        record.headers().lastHeader(EventOwnerHeader.AUTH_PARAM_NAME).value(),
                        Charsets.UTF_8));
        Assert.assertEquals(VALUE,
                new String(
                        record.headers().lastHeader(EventOwnerHeader.AUTH_PARAM_VALUE).value(),
                        Charsets.UTF_8));
    }

    @Test
    public void testEventOwnerHeaderDeserialization() {
        final ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>("topic", 1, 1L, "key".getBytes(), "value".getBytes());
        record.headers().add(EventOwnerHeader.AUTH_PARAM_NAME, NAME.getBytes(Charsets.UTF_8));
        record.headers().add(EventOwnerHeader.AUTH_PARAM_VALUE, VALUE.getBytes(Charsets.UTF_8));
        final EventOwnerHeader eventOwnerHeader = EventOwnerHeader.deserialize(record);

        Assert.assertEquals(NAME, eventOwnerHeader.getName());
        Assert.assertEquals(VALUE, eventOwnerHeader.getValue());
    }
}
