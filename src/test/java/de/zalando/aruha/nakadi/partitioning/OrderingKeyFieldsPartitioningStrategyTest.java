package de.zalando.aruha.nakadi.partitioning;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Consumer;

import static de.zalando.aruha.nakadi.utils.TestUtils.resourceAsString;
import static java.lang.Integer.parseInt;
import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.generate;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class OrderingKeyFieldsPartitioningStrategyTest {

    public static final Random RANDOM = new Random();
    public static final String DELIMITER = "#";
    public static final String[] PARTITIONS = new String[]{"0", "1", "2", "3", "4", "5", "6", "7"};

    private final OrderingKeyFieldsPartitioningStrategy strategy = new OrderingKeyFieldsPartitioningStrategy();
    private final EventType simpleEventType;
    private final ArrayList<List<JSONObject>> partitions = createEmptyPartitions(PARTITIONS.length);

    public OrderingKeyFieldsPartitioningStrategyTest() {
        simpleEventType = new EventType();
        simpleEventType.setOrderingKeyFields(asList("sku", "name"));
    }

    @Test
    public void calculatesSamePartitionForSameOrderingKeyFields() throws Exception {
        fillPartitionsWithEvents(simpleEventType, partitions, 1000);

        checkThatEventsWithSameKeysAreInSamePartition(partitions);
    }

    @Test
    public void partitionsAreEquallyDistributed() {
        // This is a probabilistic test.
        // The probability that it fails is approx. 0.577%

        fillPartitionsWithEvents(simpleEventType, partitions, 10000);

        final double[] eventDistribution = partitions.stream().map(p -> p.size()).mapToDouble(value -> value * 1.0).toArray();
        final double variance = calculateVarianceOfUniformDistribution(eventDistribution);

        assertThat(variance, lessThan(1.5));
    }

    @Test
    public void canHandleComplexKeys() throws Exception {
        final JSONObject event = new JSONObject(resourceAsString("../complex-event.json", this.getClass()));

        final EventType eventType = new EventType();
        eventType.setOrderingKeyFields(asList("sku", "brand", "category_id", "details.detail_a.detail_a_a"));

        final String partition = strategy.calculatePartition(eventType, event, PARTITIONS);

        assertThat(partition, isIn(PARTITIONS));
    }

    @Test
    @Ignore
    public void testVariance() {
        final SecureRandom random = new SecureRandom();

        final int numberOfSamples = 10000;
        final int numberOfRuns = 100 * 1000;
        final double threshold = 1.5;

        double failProbability = 0;

        for (int run = 0; run < numberOfRuns; run++) {
            final double[] dist = new double[8];
            for (int i = 0; i < numberOfSamples; i++) {
                dist[random.nextInt(dist.length)]++;
            }
            final double variance = calculateVarianceOfUniformDistribution(dist);
            //System.out.println(Arrays.toString(dist) + " = " + variance);

            if (variance > threshold) {
                failProbability += (1.0 / numberOfRuns);
            }

            if (((run * 100.0) / numberOfRuns) % 1 == 0) {
                System.out.println((int) ((run * 100.0) / numberOfRuns) + "%");
            }
        }

        System.out.println("probability to fail the test: " + failProbability);
    }


    private double calculateVarianceOfUniformDistribution(final double[] samples) {
        final double x_sum = stream(samples).sum();
        final double x_pow2_sum = stream(samples).map(d -> pow(d, 2)).sum();

        final double n = x_sum;

        final double expectedValue = (n / samples.length);


        final double variance = (1.0 / n) * (x_pow2_sum - (1.0 / n) * pow(x_sum, 2));
        return abs(variance - expectedValue);
    }

    private void checkThatEventsWithSameKeysAreInSamePartition(final List<List<JSONObject>> unsortedPartitions) {
        // Sort the event in all partitions (and keep only the key)
        final List<TreeSet<String>> partitions = sortPartitions(unsortedPartitions);

        // search for each event in all other partitions
        // foreach partition
        partitions.stream().parallel().forEach(partition ->

                // foreach event key
                partition.stream().parallel().forEach(event -> {

                            final String failMessage = "The events with the key '" + event
                                    + "' should only emerge in one partition but were found in at least two.";

                            partitions.stream().parallel()
                                    .filter(otherPartition -> otherPartition != partition)
                                    .forEach(otherPartition ->
                                            assertFalse(failMessage, otherPartition.contains(event))
                                    );
                        }
                )

        );
    }

    private List<TreeSet<String>> sortPartitions(final List<List<JSONObject>> unsortedPartitions) {
        return unsortedPartitions.stream().parallel()
                .map(jsonObjects -> jsonObjects.stream()
                        .map(jsonObject -> jsonObject.getString("sku") + DELIMITER + jsonObject.getString("name"))
                        .distinct()
                        .collect(toCollection(TreeSet::new)))
                .collect(toList());
    }

    private static ArrayList<List<JSONObject>> createEmptyPartitions(final int numberOfPartitions) {
        return generate(LinkedList<JSONObject>::new)
                .limit(numberOfPartitions)
                .collect(toCollection(ArrayList<List<JSONObject>>::new));
    }

    private void fillPartitionsWithEvents(final EventType eventType, final ArrayList<List<JSONObject>> partitions, final int numberOfEvents) {
        final int numberOfPartitions = partitions.size();

        generate(this::randomArticleEvent).limit(numberOfEvents)
                .forEach(exceptionSafe(event -> {
                    final String partition = strategy.calculatePartition(eventType, event, PARTITIONS);
                    final int partitionNo = parseInt(partition);
                    partitions.get(partitionNo).add(event);
                }));
    }

    private JSONObject randomArticleEvent() {
        return createArticleEvent(randomAlphabetic(1), RANDOM.nextInt(10), randomAlphabetic(1), randomAlphabetic(10), RANDOM.nextInt(1000));
    }

    private JSONObject createArticleEvent(final String sku, final int categoryId, final String name, final String color, final int price) {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("sku", sku);
        jsonObject.put("categoryId", categoryId);
        jsonObject.put("name", name);
        jsonObject.put("color", color);
        jsonObject.put("price", price);
        return jsonObject;
    }

    public static <T> Consumer<T> exceptionSafe(ConsumerWithException<T> consumer) {
        return t -> {
            try {
                consumer.accept(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @FunctionalInterface
    private interface ConsumerWithException<T> {
        void accept(T t) throws Exception;
    }
}