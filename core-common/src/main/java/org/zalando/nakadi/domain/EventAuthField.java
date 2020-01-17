package org.zalando.nakadi.domain;


import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Class represents serializer / deserializer of event authentication header.
 */
public class EventAuthField {

    static final String HEADER_KEY_TYPE = "X-Event-Field-Type";
    static final String HEADER_KEY_CLASSIFIER = "X-Event-Field-Classifier";

    private final String type;
    private final String classifier;

    public EventAuthField(final String type, final String classifier) {
        this.type = type;
        this.classifier = classifier;
    }

    public void serialize(final ProducerRecord<String, String> record) {
        record.headers().add(HEADER_KEY_TYPE, type.getBytes(Charsets.UTF_8));
        record.headers().add(HEADER_KEY_CLASSIFIER, classifier.getBytes(Charsets.UTF_8));
    }

    public static EventAuthField deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final Header typeHeader = record.headers().lastHeader(EventAuthField.HEADER_KEY_TYPE);
        final Header valueHeader = record.headers().lastHeader(EventAuthField.HEADER_KEY_CLASSIFIER);
        return new EventAuthField(
                new String(typeHeader.value(), Charsets.UTF_8),
                new String(valueHeader.value(), Charsets.UTF_8));
    }

    public String getType() {
        return type;
    }

    public String getClassifier() {
        return classifier;
    }
}
