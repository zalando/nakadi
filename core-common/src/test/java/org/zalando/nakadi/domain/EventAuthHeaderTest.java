package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

public class EventAuthHeaderTest {

    private static final String TYPE = "teams";
    public static final String CLASSIFIER = "nakadi";

    @Test
    public void testEventAuthHeaderSerialization() {
        final EventAuthField eventAuthField = new EventAuthField(TYPE, CLASSIFIER);
        final ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");
        eventAuthField.serialize(record);

        Assert.assertEquals(TYPE,
                new String(
                        record.headers().lastHeader(EventAuthField.HEADER_KEY_TYPE).value(),
                        Charsets.UTF_8));
        Assert.assertEquals(CLASSIFIER,
                new String(
                        record.headers().lastHeader(EventAuthField.HEADER_KEY_CLASSIFIER).value(),
                        Charsets.UTF_8));
    }

    @Test
    public void testEventAuthHeaderDeserialization() {
        final ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>("topic", 1, 1L, "key".getBytes(), "value".getBytes());
        record.headers().add(EventAuthField.HEADER_KEY_TYPE, TYPE.getBytes(Charsets.UTF_8));
        record.headers().add(EventAuthField.HEADER_KEY_CLASSIFIER, CLASSIFIER.getBytes(Charsets.UTF_8));
        final EventAuthField eventAuthField = EventAuthField.deserialize(record);

        Assert.assertEquals(TYPE, eventAuthField.getType());
        Assert.assertEquals(CLASSIFIER, eventAuthField.getClassifier());
    }
}
