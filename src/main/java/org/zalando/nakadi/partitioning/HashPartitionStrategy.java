package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.util.JsonPathAccess;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.abs;

public class HashPartitionStrategy implements PartitionStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(HashPartitionStrategy.class);

    private static final String DATA_PATH_PREFIX = JsonSchemaEnrichment.DATA_CHANGE_WRAP_FIELD + ".";

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

            final ArrayList<String> fieldValues = new ArrayList<>();
            final int hashValue = partitionKeyFields.stream()
                    // The problem is that JSONObject doesn't override hashCode(). Therefore convert it to
                    // a string first and then use hashCode()
                    .map(pkf -> EventCategory.DATA.equals(eventType.getCategory()) ? DATA_PATH_PREFIX + pkf : pkf)
                    .map(Try.wrap(okf -> {
                        final String fieldValue = traversableJsonEvent.get(okf).toString();
                        fieldValues.add(fieldValue);
                        return fieldValue.hashCode();
                    }))
                    .map(Try::getOrThrow)
                    .mapToInt(hc -> hc)
                    .sum();

            final int partitionIndex = abs(hashValue) % partitions.size();
            final String partition = partitions.get(partitionIndex);

            LOG.info("[HASH_BUG] hash - values:{} -> hash:{} -> index:{} -> partition:{}; " +
                    "partitions of '{}' [PNUM:{}]: {}", fieldValues, hashValue, partitionIndex, partition,
                    eventType.getName(), partitions.size(), partitions.stream().collect(Collectors.joining(",")));

            return partition;

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
