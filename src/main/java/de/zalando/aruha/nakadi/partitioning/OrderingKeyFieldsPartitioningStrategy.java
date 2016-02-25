package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.ExceptionWrapper;
import de.zalando.aruha.nakadi.exceptions.InvalidOrderingKeyFieldsException;
import de.zalando.aruha.nakadi.util.JsonPathAccess;
import org.json.JSONObject;

import java.util.List;

import static de.zalando.aruha.nakadi.exceptions.ExceptionWrapper.wrapFunction;
import static java.lang.Math.abs;

public class OrderingKeyFieldsPartitioningStrategy implements PartitioningStrategy {

    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final String[] partitions) throws InvalidOrderingKeyFieldsException {
        final List<String> orderingKeyFields = eventType.getOrderingKeyFields();
        if (orderingKeyFields.isEmpty()) {
            throw new RuntimeException("Applying " + this.getClass().getSimpleName() + " although event type " +
                    "has no ordering key fields configured.");
        }

        try {

            final JsonPathAccess traversableJsonEvent = new JsonPathAccess(event);

            final int hashValue = orderingKeyFields.stream()
                    // The problem is that JSONObject doesn't override hashCode(). Therefore convert it to
                    // a string first and then use hashCode()
                    .map(wrapFunction(okf -> traversableJsonEvent.get(okf).toString().hashCode()))
                    .mapToInt(hc -> hc)
                    .sum();

            final int partitionIndex = abs(hashValue) % partitions.length;
            return partitions[partitionIndex];

        } catch (ExceptionWrapper e) {
            final Exception original = e.getWrapped();
            if (original instanceof InvalidOrderingKeyFieldsException) {
                throw (InvalidOrderingKeyFieldsException) original;
            } else {
                throw e;
            }
        }
    }


}
