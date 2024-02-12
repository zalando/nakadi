package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.EnumMap;
import java.util.Map;

public class KafkaHeaderTagSerde {

    public static void serialize(final Map<HeaderTag, String> consumerTags,
                                 final ProducerRecord<byte[], byte[]> record) {
        consumerTags.forEach((tag, value) ->
                record.headers().add(tag.name(), value.getBytes(Charsets.UTF_8)));
    }

    public static Map<HeaderTag, String> deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final Map<HeaderTag, String> result = new EnumMap<>(HeaderTag.class);
        record.headers().forEach(header -> {
            HeaderTag.fromString(header.key()).ifPresent(tag ->
                    result.put(tag, new String(header.value(), Charsets.UTF_8))
            );
        });
        return result;
    }
}
