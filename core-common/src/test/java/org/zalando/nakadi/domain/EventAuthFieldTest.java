package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

public class EventAuthFieldTest {

    private static final String TYPE = "teams";
    public static final String CLASSIFIER = "nakadi";

    @Test
    public void testEventAuthHeaderSerialization() {
        final EventAuthField eventAuthField = new EventAuthField(TYPE, CLASSIFIER);
        final ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");
        eventAuthField.serialize(record);

        Assert.assertEquals(TYPE,
                new String(
                        record.headers().lastHeader(EventAuthField.AUTH_PARAM_NAME).value(),
                        Charsets.UTF_8));
        Assert.assertEquals(CLASSIFIER,
                new String(
                        record.headers().lastHeader(EventAuthField.AUTH_PARAM_VALUE).value(),
                        Charsets.UTF_8));
    }

    @Test
    public void testEventAuthHeaderDeserialization() {
        final ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>("topic", 1, 1L, "key".getBytes(), "value".getBytes());
        record.headers().add(EventAuthField.AUTH_PARAM_NAME, TYPE.getBytes(Charsets.UTF_8));
        record.headers().add(EventAuthField.AUTH_PARAM_VALUE, CLASSIFIER.getBytes(Charsets.UTF_8));
        final EventAuthField eventAuthField = EventAuthField.deserialize(record);

        Assert.assertEquals(TYPE, eventAuthField.getName());
        Assert.assertEquals(CLASSIFIER, eventAuthField.getValue());
    }
}
