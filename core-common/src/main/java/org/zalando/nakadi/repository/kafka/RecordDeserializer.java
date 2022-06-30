package org.zalando.nakadi.repository.kafka;

public interface RecordDeserializer {
    byte[] deserializeToJsonBytes(byte[] data);
}
