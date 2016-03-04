package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.ExceptionWrapper;
import de.zalando.aruha.nakadi.exceptions.InvalidPartitioningKeyFieldsException;
import de.zalando.aruha.nakadi.util.JsonPathAccess;
import org.json.JSONObject;

import java.util.List;

import static de.zalando.aruha.nakadi.exceptions.ExceptionWrapper.wrapFunction;
import static java.lang.Math.abs;

public class PartitioningKeyFieldsPartitioningStrategy implements PartitioningStrategy {

    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final List<String> partitions) throws InvalidPartitioningKeyFieldsException {
        final List<String> partitioningKeyFields = eventType.getPartitioningKeyFields();
        if (partitioningKeyFields.isEmpty()) {
            throw new RuntimeException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no partitioning key fields configured.");
        }

        try {

            final JsonPathAccess traversableJsonEvent = new JsonPathAccess(event);

            final int hashValue = partitioningKeyFields.stream()
                    // The problem is that JSONObject doesn't override hashCode(). Therefore convert it to
                    // a string first and then use hashCode()
                    .map(wrapFunction(okf -> traversableJsonEvent.get(okf).toString().hashCode()))
                    .mapToInt(hc -> hc)
                    .sum();

            final int partitionIndex = abs(hashValue) % partitions.size();
            return partitions.get(partitionIndex);

        } catch (ExceptionWrapper e) {
            final Exception original = e.getWrapped();
            if (original instanceof InvalidPartitioningKeyFieldsException) {
                throw (InvalidPartitioningKeyFieldsException) original;
            } else {
                throw e;
            }
        }
    }


}
