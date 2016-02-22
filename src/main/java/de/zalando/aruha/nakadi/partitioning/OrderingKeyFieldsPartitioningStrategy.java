package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.util.List;

import static java.lang.Math.abs;

public class OrderingKeyFieldsPartitioningStrategy implements PartitioningStrategy {

    @Override
    public String calculatePartition(final EventType eventType, final JSONObject event, final int numberOfPartitions) throws InvalidOrderingKeyFieldsException {
        try {

            final List<String> orderingKeyFields = eventType.getOrderingKeyFields();

            final JsonPath traversableJsonEvent = new JsonPath(event);

            int hashValue = orderingKeyFields.stream()
                    // The problem is that JSONObject doesn't override hashCode(). Therefore convert it to
                    // a string first and then use hashCode()
                    .map(okf -> traversableJsonEvent.get(okf).toString().hashCode())
                    .mapToInt(hc -> hc)
                    .sum();

            return String.valueOf(abs(hashValue) % numberOfPartitions);

        } catch (RuntimeException e) {
            final Throwable cause = e.getCause();
            if (cause != null && cause instanceof InvalidOrderingKeyFieldsException) {
                throw (InvalidOrderingKeyFieldsException) cause;
            } else {
                throw e;
            }
        }
    }

    /*
     One could use JsonPath instead: https://github.com/jayway/JsonPath

     However, I had the feeling that JsonPath is way too much compared to what is needed here. Therefore it might be
     slower. Moreover we already have a JSONObject. Using JsonPath would mean to convert it back to a string (or pass
     the json string instead of the JSONObject to the strategy) and parse it again.
     */
    private static class JsonPath {
        private final JSONObject jsonObject;

        private JsonPath(final JSONObject jsonObject) {
            this.jsonObject = jsonObject;
        }

        public Object get(final String path) {
            final String[] fields = StringUtils.split(path, '.');

            Object curr = this.jsonObject;
            for (String field : fields) {
                if (!(curr instanceof JSONObject)) {
                    throw new RuntimeException(new InvalidOrderingKeyFieldsException("field " + field + "doesn't exist."));
                }
                curr = ((JSONObject) curr).opt(field);
            }
            return curr;
        }
    }
}
