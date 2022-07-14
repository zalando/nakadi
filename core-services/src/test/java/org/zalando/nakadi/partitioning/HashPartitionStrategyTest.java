package org.zalando.nakadi.partitioning;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HashPartitionStrategyTest {

    private static final Random RANDOM = new Random();
    private static final String DELIMITER = "#";
    private static final String[] PARTITIONS = new String[]{"0", "1", "2", "3", "4", "5", "6", "7"};

    private static List<JSONObject> eventSamplesA = null;
    private static List<JSONObject> eventSamplesB = null;
    private static List<JSONObject> eventSamplesC = null;
    private final EventType simpleEventType;

    private final HashPartitionStrategyCrutch hashPartitioningCrutch;
    private final HashPartitionStrategy strategy;

    public HashPartitionStrategyTest() {
        simpleEventType = new EventType();
        simpleEventType.setPartitionKeyFields(asList("sku", "name"));

        hashPartitioningCrutch = mock(HashPartitionStrategyCrutch.class);
        when(hashPartitioningCrutch.adjustPartitionIndex(anyInt(), anyInt()))
                .thenAnswer(invocation -> invocation.getArguments()[0]); // don't do any adjustments

        strategy = new HashPartitionStrategy(hashPartitioningCrutch, new StringHash());
    }

    @Test
    public void whenCorrectPartitionFieldsKeysThenOk() {
        final var metadata = Mockito.mock(NakadiMetadata.class);
        when(metadata.getPartitionKeys()).thenReturn(List.of("sku=123", "price=512$"));

        final var partitions = List.of("0", "1", "2");

        assertEquals("2", strategy.calculatePartition(metadata, partitions));
    }

    @Test(expected = PartitioningException.class)
    public void whenPartitionKeysMissingInMetadataThenThrow() {
        final var metadata = Mockito.mock(NakadiMetadata.class);
        when(metadata.getPartitionKeys()).thenReturn(Collections.emptyList());

        final var partitions = List.of("0", "1", "2");

        strategy.calculatePartition(metadata, partitions);
    }

    @Test
    public void calculatesSamePartitionForSamePartitionKeyFields() throws Exception {
        final ArrayList<List<JSONObject>> partitions = createEmptyPartitions(PARTITIONS.length);
        fillPartitionsWithRandomEvents(simpleEventType, partitions, 1000);

        checkThatEventsWithSameKeysAreInSamePartition(partitions);
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
        final ArrayList<List<JSONObject>> partitions = createEmptyPartitions(PARTITIONS.length);
        fillPartitionsWithEvents(simpleEventType, partitions, events);

        final double[] eventDistribution = partitions.stream().map(List::size).mapToDouble(value -> value * 1.0)
                .toArray();
        return calculateVarianceOfUniformDistribution(eventDistribution);
    }

    @Test
    public void canHandleComplexKeys() throws Exception {
        assertThat(strategy.calculatePartition(List.of("a", "b", "c", "d"), asList(PARTITIONS)), isIn(PARTITIONS));
    }

    @Test
    public void whenStringHashCodeIsIntMinThenItWorks() throws Exception {
        final var partitionKeys = List.of("sku=ABC-123");
        final String[] partitions = new String[]{"0", "1", "2"};

        final StringHash stringHash = mock(StringHash.class);
        final HashPartitionStrategy strategy = new HashPartitionStrategy(hashPartitioningCrutch, stringHash);
        when(stringHash.hashCode(anyString())).thenReturn(Integer.MIN_VALUE);

        assertEquals("2", strategy.calculatePartition(partitionKeys, asList(partitions)));
    }

    @Test
    public void usesProvidedStringHash() {
        final StringHash stringHashMock = mock(StringHash.class);

        when(stringHashMock.hashCode("key1")).thenReturn(2);
        when(stringHashMock.hashCode("key2")).thenReturn(3);

        final HashPartitionStrategy mockedStringHashStrategy =
                new HashPartitionStrategy(hashPartitioningCrutch, stringHashMock);

        final List<String> partitionNames = asList(PARTITIONS);
        final List<String> resultPartitions =
                List.of(List.of("key1"),
                        List.of("key2"),
                        List.of("key1", "key2"))
                .stream()
                .map(keys -> mockedStringHashStrategy.calculatePartition(keys, partitionNames))
                .collect(Collectors.toList());

        assertThat(resultPartitions, equalTo(List.of("2", "3", "5")));

        verify(stringHashMock, times(2)).hashCode(eq("key1"));
        verify(stringHashMock, times(2)).hashCode(eq("key2"));
    }

    @Test(expected = PartitioningException.class)
    public void whenPartitionKeyFieldsAreMissingThenItThrows() {
        strategy.calculatePartition(mock(BatchItem.class), Collections.emptyList());
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
        return unsortedPartitions
                .stream()
                .parallel()
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

        final List<String> partitionNames = asList(PARTITIONS);

        for (final JSONObject event : events) {
            final List<String> keys = List.of(event.getString("sku"), event.getString("name"));

            final String partition = strategy.calculatePartition(keys, partitionNames);
            final int partitionNo = parseInt(partition);

            partitions.get(partitionNo).add(event);
        }
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
