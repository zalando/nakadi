package org.zalando.nakadi.domain;


import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Class represents serializer / deserializer of event authentication header.
 */
public class EventAuthField {

    static final String AUTH_PARAM_NAME = "X-AuthParam-Name";
    static final String AUTH_PARAM_VALUE = "X-AuthParam-Value";

    private final String name;
    private final String value;

    public EventAuthField(final String name, final String value) {
        this.name = name;
        this.value = value;
    }

    public void serialize(final ProducerRecord<String, String> record) {
        record.headers().add(AUTH_PARAM_NAME, name.getBytes(Charsets.UTF_8));
        record.headers().add(AUTH_PARAM_VALUE, value.getBytes(Charsets.UTF_8));
    }

    public static EventAuthField deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final Header typeHeader = record.headers().lastHeader(EventAuthField.AUTH_PARAM_NAME);
        if (null == typeHeader) {
            return null;
        }
        final Header valueHeader = record.headers().lastHeader(EventAuthField.AUTH_PARAM_VALUE);
        if (valueHeader == null) {
            return null;
        }

        return new EventAuthField(
                new String(typeHeader.value(), Charsets.UTF_8),
                new String(valueHeader.value(), Charsets.UTF_8));
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
