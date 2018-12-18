package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
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
    public String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions)
            throws InvalidPartitionKeyFieldsException {
        final List<String> partitionKeyFields = eventType.getPartitionKeyFields();
        if (partitionKeyFields.isEmpty()) {
            throw new RuntimeException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partition key fields configured.");
        }

        try {

            final JsonPathAccess traversableJsonEvent = new JsonPathAccess(event);

            final int hashValue = partitionKeyFields.stream()
                    // The problem is that JSONObject doesn't override hashCode(). Therefore convert it to
                    // a string first and then use hashCode()
                    .map(pkf -> EventCategory.DATA.equals(eventType.getCategory()) ? DATA_PATH_PREFIX + pkf : pkf)
                    .map(Try.wrap(okf -> {
                        final String fieldValue = traversableJsonEvent.get(okf).toString();
                        return stringHash.hashCode(fieldValue);
                    }))
                    .map(Try::getOrThrow)
                    .mapToInt(hc -> hc)
                    .sum();


            int partitionIndex = abs(hashValue) % partitions.size();
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

}
