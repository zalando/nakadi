package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordDeserializer {
    byte[] deserialize(final ConsumerRecord<byte[], byte[]> record);
}
