package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaConsumerTagSerializer {

    private static final Map<String, ConsumerTag> STRING_TO_ENUM = ConsumerTag.
            stream().
            collect(Collectors.toMap(ConsumerTag::name, Function.identity()));

    public static void serialize(final Map<ConsumerTag, String> consumerTags,
                                 final ProducerRecord<byte[], byte[]> record) {
        consumerTags.
                forEach((tag, value) -> record.headers().add(tag.name(), value.getBytes(Charsets.UTF_8)));

    }

    public static Map<ConsumerTag, String> deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final var result = new HashMap<ConsumerTag, String>();
        record.headers().forEach(header -> {
            if(STRING_TO_ENUM.containsKey(header.key())){
                result.put(STRING_TO_ENUM.get(header.key()),
                        new String(header.value(), Charsets.UTF_8));
            }
        });
        return result;
    }
}
