package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class KafkaSubscriptionSerializer {
    private static String SUBSCRIPTION_ID = "DLQ_SUBSCRIPTION_ID";

    public static void serialize(final String subscriptionId, final ProducerRecord<byte[], byte[]> record) {
        record.headers().add(SUBSCRIPTION_ID, subscriptionId.getBytes(Charsets.UTF_8));
    }

    public static String deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final Header subscriptionIdHeader = record.headers().lastHeader(SUBSCRIPTION_ID);
        return null == subscriptionIdHeader? null:
                new String(subscriptionIdHeader.value(), Charsets.UTF_8);
    }
}
