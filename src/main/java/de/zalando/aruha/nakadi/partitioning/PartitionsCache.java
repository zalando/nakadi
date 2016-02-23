package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.kafka.KafkaFactory;
import kafka.common.KafkaException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionsCache {

    private final Map<String, String[]> partitionsPerEventType = new ConcurrentHashMap<>();

    private final KafkaFactory kafkaFactory;

    public PartitionsCache(final KafkaFactory kafkaFactory) {
        this.kafkaFactory = kafkaFactory;
    }

    public String[] getPartitionsFor(final String eventTypeName) throws ServiceUnavailableException {
        try {
            String[] partitions = partitionsPerEventType.get(eventTypeName);

            if (partitions == null) {
                partitions = kafkaFactory.createProducer().partitionsFor(eventTypeName)
                        .stream()
                        .map(partitionInfo -> String.valueOf(partitionInfo.partition()))
                        .toArray(n -> new String[n]);
                partitionsPerEventType.put(eventTypeName, partitions);
            }

            return partitions;
        } catch (final KafkaException e) {
            throw new ServiceUnavailableException("Could not get partition information about '" + eventTypeName + "'", e);
        }
    }

}
