package org.zalando.nakadi.repository.kafka;

public interface RecordDeserializer {
    byte[] deserialize(byte[] eventFormat, byte[] data);
}
