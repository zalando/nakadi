package org.zalando.nakadi.partitioning;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.Try;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
import static org.zalando.nakadi.utils.TestUtils.resourceAsString;

public class HashPartitionStrategyTest {

    private static final Random RANDOM = new Random();
    private static final String DELIMITER = "#";
    private static final String[] PARTITIONS = new String[]{"0", "1", "2", "3", "4", "5", "6", "7"};

    private static List<JSONObject> eventSamplesA = null;
    private static List<JSONObject> eventSamplesB = null;
    private static List<JSONObject> eventSamplesC = null;

    private final HashPartitionStrategy strategy = new HashPartitionStrategy();
    private final EventType simpleEventType;
    private final ArrayList<List<JSONObject>> partitions = createEmptyPartitions(PARTITIONS.length);

    public HashPartitionStrategyTest() {
        simpleEventType = new EventType();
        simpleEventType.setPartitionKeyFields(asList("sku", "name"));
    }

    @Test
    public void calculatesSamePartitionForSamePartitionKeyFields() throws Exception {
        fillPartitionsWithRandomEvents(simpleEventType, partitions, 1000);

        checkThatEventsWithSameKeysAreInSamePartition(partitions);
    }

    @Test
    @Ignore("This might be useful to play around with for future implementations of PartitionStrategies")
    public void partitionsAreEvenlyDistributedUsingRandomEvents() {
        // This is a probabilistic test.
        // The probability that it fails is approx. 0.577%

        fillPartitionsWithRandomEvents(simpleEventType, partitions, 10000);

        final double[] eventDistribution = partitions.stream().map(List::size).mapToDouble(value -> value * 1.0)
                .toArray();
        final double variance = calculateVarianceOfUniformDistribution(eventDistribution);

        assertThat(variance, lessThan(1.5));
    }

    @Test
    public void partitionsAreEvenlyDistributed() throws IOException {
        loadEventSamples();

        assertThat("Event sample set A is not evenly distributed with strategy", varianceForEvents(eventSamplesA),
                lessThan(1.5));
        assertThat("Event sample set B is not evenly distributed with strategy", varianceForEvents(eventSamplesB),
                lessThan(1.5));
        assertThat("Event sample set C is not evenly distributed with strategy", varianceForEvents(eventSamplesC),
                lessThan(1.5));
    }

    private double varianceForEvents(final List<JSONObject> events) {
        fillPartitionsWithEvents(simpleEventType, partitions, events);

        final double[] eventDistribution = partitions.stream().map(List::size).mapToDouble(value -> value * 1.0)
                .toArray();
        return calculateVarianceOfUniformDistribution(eventDistribution);
    }

    @Test
    @Ignore("Run this to create a new set of event samples")
    public void createSampleSet() {
        final List<JSONObject> events = generateRandomEvents(10000);

        for (final JSONObject event : events) {
            System.out.println(event.toString());
        }
    }

    @Test
    public void canHandleComplexKeys() throws Exception {
        final JSONObject event = new JSONObject(resourceAsString("../complex-event.json", this.getClass()));

        final EventType eventType = new EventType();
        eventType.setPartitionKeyFields(asList("sku", "brand", "category_id", "details.detail_a.detail_a_a"));

        final String partition = strategy.calculatePartition(eventType, event, asList(PARTITIONS));

        assertThat(partition, isIn(PARTITIONS));
    }

    @Test
    @Ignore("Tests the variance used in the tests here")
    public void testVariance() {
        final SecureRandom random = new SecureRandom();

        final int numberOfSamples = 100000;
        final int numberOfRuns = 1000;
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
        final double xSum = stream(samples).sum();
        final double xPow2Sum = stream(samples).map(d -> pow(d, 2)).sum();

        final double n = xSum;

        final double expectedValue = (n / samples.length);


        final double variance = (1.0 / n) * (xPow2Sum - (1.0 / n) * pow(xSum, 2));
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

    private List<JSONObject> generateRandomEvents(final int numberOfEvents) {
        return generate(this::randomArticleEvent).limit(numberOfEvents).collect(toList());
    }

    private void fillPartitionsWithRandomEvents(final EventType eventType, final ArrayList<List<JSONObject>> partitions,
                                                final int numberOfEvents) {
        fillPartitionsWithEvents(eventType, partitions, generateRandomEvents(numberOfEvents));
    }

    private void fillPartitionsWithEvents(final EventType eventType, final ArrayList<List<JSONObject>> partitions,
                                          final List<JSONObject> events) {
        events.stream()
                .map(Try.<JSONObject, Void>wrap(event -> {
                    final String partition = strategy.calculatePartition(eventType, event, asList(PARTITIONS));
                    final int partitionNo = parseInt(partition);
                    partitions.get(partitionNo).add(event);
                    return null;
                }).andThen(Try::getOrThrow)).collect(Collectors.toSet());
    }

    private JSONObject randomArticleEvent() {
        return createArticleEvent(randomAlphabetic(1), RANDOM.nextInt(10), randomAlphabetic(1), randomAlphabetic(10),
                RANDOM.nextInt(1000));
    }

    private JSONObject createArticleEvent(final String sku, final int categoryId, final String name, final String color,
                                          final int price) {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("sku", sku);
        jsonObject.put("categoryId", categoryId);
        jsonObject.put("name", name);
        jsonObject.put("color", color);
        jsonObject.put("price", price);
        return jsonObject;
    }

    private void loadEventSamples() throws IOException {
        if (eventSamplesA == null) {
            eventSamplesA = loadEventSampleSet("events.10000.A.txt");
        }
        if (eventSamplesB == null) {
            eventSamplesB = loadEventSampleSet("events.10000.B.txt");
        }
        if (eventSamplesC == null) {
            eventSamplesC = loadEventSampleSet("events.10000.C.txt");
        }
    }

    private List<JSONObject> loadEventSampleSet(final String sampleSetName) throws IOException {
        final InputStream in = this.getClass().getResourceAsStream(sampleSetName);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line;
        final List<JSONObject> events = new ArrayList<>(10000);
        while ((line = reader.readLine()) != null) {
            if (StringUtils.isNoneBlank(line)) {
                events.add(new JSONObject(line));
            }
        }
        return events;
    }

}