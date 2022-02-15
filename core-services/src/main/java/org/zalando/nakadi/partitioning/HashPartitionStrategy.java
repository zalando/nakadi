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
import org.zalando.nakadi.util.JsonPathAccess;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.abs;
import static org.zalando.nakadi.validation.JsonSchemaEnrichment.DATA_PATH_PREFIX;

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
    public List<String> extractEventKeys(final EventType eventType, final JSONObject event)
            throws InvalidPartitionKeyFieldsException {

        final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
        if (partitionKeyFields.isEmpty()) {
            throw new RuntimeException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition key fields configured.");
        }
        try {
            final JsonPathAccess traversableJsonEvent = new JsonPathAccess(event);
            return partitionKeyFields.stream()
                    .map(pkf -> EventCategory.DATA.equals(eventType.getCategory()) ? DATA_PATH_PREFIX + pkf : pkf)
                    .map(Try.wrap(okf -> {
                        try {
                            return traversableJsonEvent.get(okf).toString();
                        } catch (final JsonPathAccessException e) {
                            throw new InvalidPartitionKeyFieldsException(e.getMessage());
                        }
                    }))
                    .map(Try::getOrThrow)
                    .collect(Collectors.toList());

        } catch (NakadiRuntimeException e) {
            final Exception original = e.getException();
            if (original instanceof InvalidPartitionKeyFieldsException) {
                throw (InvalidPartitionKeyFieldsException) original;
            } else {
                throw e;
            }
        }
    }

    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions)
            throws InvalidPartitionKeyFieldsException {

        final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
        if (partitionKeyFields.isEmpty()) {
            throw new RuntimeException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition key fields configured.");
        }

        final int hashValue = extractEventKeys(eventType, event).stream()
                .map(s -> s.hashCode())
                .mapToInt(hc -> hc)
                .sum();

        int partitionIndex = abs(hashValue) % partitions.size();
        partitionIndex = hashPartitioningCrutch.adjustPartitionIndex(partitionIndex, partitions.size());

        // TODO: pre-sort
        final List<String> sortedPartitions = partitions.stream().sorted().collect(Collectors.toList());
        return sortedPartitions.get(partitionIndex);
    }
}
