package org.zalando.nakadi.domain;


import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Objects;

/**
 * Class represents serializer / deserializer of event authentication header.
 */
public class EventOwnerHeader {

    public static final String AUTH_PARAM_NAME = "X-AuthParam-Name";
    public static final String AUTH_PARAM_VALUE = "X-AuthParam-Value";

    private final String name;
    private final String value;

    public EventOwnerHeader(final String name, final String value) {
        this.name = name;
        this.value = value;
    }

    public void serialize(final ProducerRecord<byte[], byte[]> record) {
        record.headers().add(AUTH_PARAM_NAME, name.getBytes(Charsets.UTF_8));
        record.headers().add(AUTH_PARAM_VALUE, value.getBytes(Charsets.UTF_8));
    }

    public static EventOwnerHeader deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final Header nameHeader = record.headers().lastHeader(EventOwnerHeader.AUTH_PARAM_NAME);
        if (null == nameHeader) {
            return null;
        }
        final Header valueHeader = record.headers().lastHeader(EventOwnerHeader.AUTH_PARAM_VALUE);
        if (valueHeader == null) {
            return null;
        }

        return new EventOwnerHeader(
                new String(nameHeader.value(), Charsets.UTF_8),
                new String(valueHeader.value(), Charsets.UTF_8));
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EventOwnerHeader that = (EventOwnerHeader) o;
        return Objects.equals(this.name, that.name) && Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
