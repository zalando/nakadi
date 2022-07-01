package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
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
                                     final List<String> orderedPartitions)
            throws PartitioningException {
        final var userDefinedPartitionKeys = getPartitionKeys(eventType, jsonEvent);

        return calculatePartition(userDefinedPartitionKeys, orderedPartitions);
    }

    @Override
    public String calculatePartition(final NakadiMetadata nakadiRecordMetadata,
                                     final List<String> orderedPartitions)
            throws PartitioningException {
        final var userDefinedPartitionKeys = nakadiRecordMetadata.getPartitionKeys();

        return calculatePartition(userDefinedPartitionKeys, orderedPartitions);
    }

    public String calculatePartition(final List<String> partitionKeys,
                                     final List<String> orderedPartitions)
            throws PartitioningException {
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            throw new PartitioningException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition keys.");
        }

        try {
            final int hashValue = partitionKeys.stream()
                    .map(Try.wrap(pkf -> stringHash.hashCode(pkf)))
                    .map(Try::getOrThrow)
                    .mapToInt(hc -> hc)
                    .sum();

            int partitionIndex = abs(hashValue % orderedPartitions.size());
            partitionIndex = hashPartitioningCrutch.adjustPartitionIndex(partitionIndex, orderedPartitions.size());

            return orderedPartitions.get(partitionIndex);

        } catch (NakadiRuntimeException e) {
            final Exception original = e.getException();
            if (original instanceof InvalidPartitionKeyFieldsException) {
                throw (InvalidPartitionKeyFieldsException) original;
            } else {
                throw e;
            }
        }
    }

    private List<String> getPartitionKeys(
            final EventType eventType,
            final JSONObject jsonEvent) throws InvalidPartitionKeyFieldsException {
        final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
        if (partitionKeyFields.isEmpty()) {
            throw new PartitioningException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition key fields configured.");
        }

        final JsonPathAccess traversableJsonEvent = new JsonPathAccess(jsonEvent);
        final var partitionKeys = partitionKeyFields
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

        return partitionKeys;
    }
}
