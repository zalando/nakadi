package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.util.JsonPathAccess;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.abs;

@Component
public class HashPartitionStrategy implements PartitionStrategy {

    private final HashPartitionStrategyCrutch hashPartitioningCrutch;
    private final StringHash stringHash;

    @Autowired
    public HashPartitionStrategy(final HashPartitionStrategyCrutch hashPartitioningCrutch,
                                 final StringHash stringHash) {
        this.hashPartitioningCrutch = hashPartitioningCrutch;
        this.stringHash = stringHash;
    }

    @Override
    public String calculatePartition(final EventType eventType,
                                     final JSONObject jsonEvent,
                                     final List<String> partitions)
            throws PartitioningException {
        final var partitioningData = getPartitionKeys(eventType, jsonEvent);

        return calculatePartition(partitioningData, partitions);
    }

    private String calculatePartition(final PartitioningData partitioningData, final List<String> partitions) {
        if (partitioningData.getPartitionKeys() == null || partitioningData.getPartitionKeys().isEmpty()) {
            throw new PartitioningException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition keys.");
        }

        try {
            final int hashValue = partitioningData.getPartitionKeys().stream()
                    .map(Try.wrap(pkf -> stringHash.hashCode(pkf)))
                    .map(Try::getOrThrow)
                    .mapToInt(hc -> hc)
                    .sum();

            int partitionIndex = abs(hashValue % partitions.size());
            partitionIndex = hashPartitioningCrutch.adjustPartitionIndex(partitionIndex, partitions.size());

            final List<String> sortedPartitions = partitions.stream().sorted().collect(Collectors.toList());
            return sortedPartitions.get(partitionIndex);

        } catch (NakadiRuntimeException e) {
            final Exception original = e.getException();
            if (original instanceof InvalidPartitionKeyFieldsException) {
                throw (InvalidPartitionKeyFieldsException) original;
            } else {
                throw e;
            }
        }
    }

    private PartitioningData getPartitionKeys(
            final EventType eventType,
            final JSONObject jsonEvent) throws InvalidPartitionKeyFieldsException {
        final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
        if (partitionKeyFields.isEmpty()) {
            throw new PartitioningException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition key fields configured.");
        }

        final JsonPathAccess traversableJsonEvent = new JsonPathAccess(jsonEvent);
        final var partitionKeys = eventType
                .getPartitionKeyFields()
                .stream()
                .map(pkf -> EventCategory.DATA.equals(eventType.getCategory())
                        ? EventType.DATA_PATH_PREFIX + pkf
                        : pkf)
                .map(Try.wrap(okf -> {
                    try {
                        return traversableJsonEvent.get(okf).toString();
                    } catch (final JsonPathAccessException e) {
                        throw new InvalidPartitionKeyFieldsException(e.getMessage());
                    }
                }))
                .map(Try::getOrThrow)
                .collect(Collectors.toList());

        return new PartitioningData()
                .setPartitionKeys(partitionKeys);
    }
}
