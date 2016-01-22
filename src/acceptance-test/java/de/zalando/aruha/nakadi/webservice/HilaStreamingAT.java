package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
import de.zalando.aruha.nakadi.utils.TestUtils;
import de.zalando.aruha.nakadi.webservice.utils.TestHelper;
import de.zalando.aruha.nakadi.webservice.utils.TestStreamingClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.fail;

public class HilaStreamingAT extends BaseAT {

    private static final int PARTITION_HALF_NUM = PARTITION_NUM / 2;
    public static final String DUMMY_EVENT = "\"Dummy\"";

    private static final Set<String> ALL_PARTITIONS = IntStream
            .range(0, PARTITION_NUM)
            .boxed()
            .map(x -> Integer.toString(x))
            .collect(Collectors.toSet());

    private TestHelper testHelper;
    private ObjectMapper jsonMapper;

    @Before
    public void setup() {
        testHelper = new TestHelper(URL);
        jsonMapper = new ObjectMapper();
    }

    /**
     * This test creates a subscription, pushes 5 messages to appropriate topic, starts a short stream for it, checks if
     * messages were consumed; after that commits the consumed offset, pushes new 3 messages, creates a short stream for
     * that subscription and checks that only 3 new messages were consumed by new stream.
     *
     * @throws InterruptedException
     */
    @Test(timeout = 12000)
    @SuppressWarnings("unchecked")
    public void whenCommitOffsetsThenStreamFromNewOffsets() throws InterruptedException {

        final String testPartition = "0";
        final String subscriptionId = TestUtils.randomString();
        final boolean created = testHelper.createSubscription(subscriptionId, ImmutableList.of(TOPIC));
        assertThat("New subscription should be created", created, is(true));

        testHelper.pushEventsToPartition(TOPIC, testPartition, DUMMY_EVENT, 5);

        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(100)
                .withStreamTimeout(1)
                .build();

        final TestStreamingClient client = TestStreamingClient.of(URL, subscriptionId, config).start();
        Thread.sleep(3000);

        final List<List<String>> initialData = ImmutableList.copyOf(client.getData());
        assertThat("Only one batch should be read", initialData, hasSize(1));

        final List<String> firstBatch = initialData.get(0);
        final Map<String, Object> batchForPartition = getDataForPartition(firstBatch, testPartition);
        final List<Object> events = (List<Object>) batchForPartition.get("events");
        assertThat("5 messages should be read for partition 0", events, hasSize(5));

        final String offset = (String)((Map<String, Object>)batchForPartition.get("cursor")).get("offset");
        testHelper.commitOffsets(subscriptionId, ImmutableList.of(new Cursor(TOPIC, testPartition, offset)));
        testHelper.pushEventsToPartition(TOPIC, testPartition, DUMMY_EVENT, 3);

        client.start();
        Thread.sleep(3000);

        final List<List<String>> newData = ImmutableList.copyOf(client.getData());
        assertThat("Only one batch should be read", newData, hasSize(1));

        final List<String> secondBatch = newData.get(0);
        final Map<String, Object> dataForPartition = getDataForPartition(secondBatch, testPartition);
        final List<Object> moreEvents = (List<Object>) dataForPartition.get("events");
        assertThat("Only 3 messages should be read as the offset for the first 5 was already committed", moreEvents,
                hasSize(3));
    }

    /**
     * Test creates subscription, starts one stream for that subscription and after some time starts a seconds stream
     * and then checks that partitions were correctly redistributed between two streams
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout = 20000)
    public void whenAddSecondClientPartitionsAreRedistributed() throws IOException, InterruptedException {

        final String subscriptionId = TestUtils.randomString();
        final boolean created = testHelper.createSubscription(subscriptionId, ImmutableList.of(TOPIC));
        assertThat("New subscription should be created", created, is(true));

        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(10)
                .withBatchTimeout(5)
                .build();

        final TestStreamingClient firstClient = TestStreamingClient.of(URL, subscriptionId, config).start();

        Thread.sleep(6000);

        final List<List<String>> singleClientData = ImmutableList.copyOf(firstClient.getData());
        assertThat("One batch should be read so far", singleClientData, hasSize(1));
        final Set<String> singleClientPartitions = getPartitionsInBatches(singleClientData.get(0));
        assertThat("As we currently have only one client, it should read from all partitions", singleClientPartitions,
                equalTo(ALL_PARTITIONS));

        final TestStreamingClient secondClient = TestStreamingClient.of(URL, subscriptionId, config).start();

        Thread.sleep(6000);

        final List<List<String>> firstClientData = ImmutableList.copyOf(firstClient.getData());
        assertThat("For the first client we should already have two batches", firstClientData, hasSize(2));
        final Set<String> firstClientPartitions = getPartitionsInBatches(firstClientData.get(1));
        assertThat("There should be only half of partitions in second batch as we already had a second client at that time",
                firstClientPartitions, hasSize(PARTITION_HALF_NUM));

        final List<List<String>> secondClientData = ImmutableList.copyOf(secondClient.getData());
        assertThat("For the second client we should have only one batch", secondClientData, hasSize(1));
        final Set<String> secondClientPartitions = getPartitionsInBatches(secondClientData.get(0));
        assertThat("The batch of second client should have only half of partitions", secondClientPartitions,
                hasSize(PARTITION_HALF_NUM));

        final Set<String> commonPartitions = Sets.intersection(firstClientPartitions, secondClientPartitions);
        assertThat("There should be no common partitions for two clients", commonPartitions, hasSize(0));

        final Set<String> allPartitionsInStream = Sets.union(firstClientPartitions, secondClientPartitions);
        assertThat("Two clients should cover all partitions we have for a topic", allPartitionsInStream,
                equalTo(ALL_PARTITIONS));
    }

    @SuppressWarnings("unchecked")
    private Set<String> getPartitionsInBatches(final List<String> batches) {
        return batches
                .stream()
                .map(batch -> {
                    try {
                        final Map<String, Object> map = jsonMapper.<Map<String, Object>>readValue(batch,
                                new TypeReference<HashMap<String, Object>>() {});
                        final Map<String, Object> cursor = (Map<String, Object>) map.get("cursor");
                        return (String)cursor.get("partition");
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail("Could not deserialize stream response");
                        return null;
                    }
                })
                .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getDataForPartition(final List<String> batches, final String partition) {
        return batches
                .stream()
                .map(batch -> {
                    try {
                        return jsonMapper.<Map<String, Object>>readValue(batch,
                                new TypeReference<HashMap<String, Object>>() {
                                });

                    } catch (IOException e) {
                        e.printStackTrace();
                        fail("Could not deserialize stream response");
                        return null;
                    }
                })
                .filter(data -> {
                            final Map<String, Object> cursor = (Map<String, Object>) data.get("cursor");
                            return partition.equals(cursor.get("partition"));
                        })
                .findFirst()
                .orElse(ImmutableMap.of());
    }

}
