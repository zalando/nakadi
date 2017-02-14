package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.repository.kafka.KafkaTopicRepository;

public class TopicRepositoryFactory {

    public static TopicRepository createTopicRepository(final Storage storage) {
        switch (storage.getType()) {
            case KAFKA:
                final KafkaTopicRepository kafkaTopicRepository = new KafkaTopicRepository();
                return kafkaTopicRepository;
        }
        throw new IllegalStateException();
    }
}
