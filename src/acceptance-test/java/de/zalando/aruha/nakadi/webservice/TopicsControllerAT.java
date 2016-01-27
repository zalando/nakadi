package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.webservice.utils.KafkaHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TopicsControllerAT extends BaseAT {

    private static final TypeReference<ArrayList<HashMap<String, String>>> LIST_OF_MAPS_REF =
            new TypeReference<ArrayList<HashMap<String, String>>>() {};

    private static final TypeReference<HashMap<String, String>> MAP_REF =
            new TypeReference<HashMap<String, String>>() {};

    private ObjectMapper jsonMapper = new ObjectMapper();

    private KafkaHelper kafkaHelper;

    private Map<String, List<PartitionInfo>> actualTopics;

    @Before
    public void setup() {
        kafkaHelper = new KafkaHelper(kafkaUrl);
        actualTopics = kafkaHelper.createConsumer().listTopics();
    }

    @Test
    public void whenListTopicsThenOk() throws IOException {
        // ACT //
        final Response response = when().get("/topics");

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value());

        final List<Map<String, String>> topicsList = asMapsList(response.print());
        assertThat(topicsList, hasSize(actualTopics.size()));

        final Set<String> topics = topicsList
                .stream()
                .map(map -> map.get("name"))
                .collect(Collectors.toSet());
        assertThat(topics, equalTo(actualTopics.keySet()));
    }

    @Test
    public void whenListPartitionsThenOk() throws IOException {
        // ACT //
        final Response response = when().get(String.format("/topics/%s/partitions", TOPIC));

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value());

        final List<Map<String, String>> partitionsList = asMapsList(response.print());
        partitionsList.forEach(this::validatePartitionStructure);

        final Set<String> partitions = partitionsList
                .stream()
                .map(map -> map.get("partition"))
                .collect(Collectors.toSet());
        final Set<String> actualPartitions = actualTopics
                .get(TOPIC)
                .stream()
                .map(pInfo -> Integer.toString(pInfo.partition()))
                .collect(Collectors.toSet());
        assertThat(partitions, equalTo(actualPartitions));
    }

    @Test
    public void whenListPartitionsAndWriteMessageThenOffsetInPartitionIsIncreased() throws ExecutionException,
            InterruptedException, IOException {
        // ACT //
        final String url = String.format("/topics/%s/partitions", TOPIC);
        final List<Map<String, String>> partitionsInfoBefore = asMapsList(get(url).print());

        writeMessageToPartition(0);
        final List<Map<String, String>> partitionsInfoAfter = asMapsList(get(url).print());

        // ASSERT //
        final Map<String, String> partitionInfoBefore = getPartitionMapByPartition(partitionsInfoBefore, "0");
        final Map<String, String> partitionInfoAfter = getPartitionMapByPartition(partitionsInfoAfter, "0");
        validateOffsetIncreasedBy(partitionInfoBefore, partitionInfoAfter, 1);
    }

    @Test
    public void whenGetPartitionThenOk() throws IOException {
        // ACT //
        final Response response = when().get(String.format("/topics/%s/partitions/0", TOPIC));

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value());
        validatePartitionStructure(asMap(response.print()));
    }

    @Test
    public void whenGetPartitionAndWriteMessageThenOffsetInPartitionIsIncreased() throws ExecutionException,
            InterruptedException, IOException {
        // ACT //
        final String url = String.format("/topics/%s/partitions/0", TOPIC);
        final Map<String, String> partitionInfoBefore = asMap(get(url).print());

        writeMessageToPartition(0);
        final Map<String, String> partitionInfoAfter = asMap(get(url).print());

        // ASSERT //
        validateOffsetIncreasedBy(partitionInfoBefore, partitionInfoAfter, 1);
    }

    private Map<String, String> getPartitionMapByPartition(final List<Map<String, String>> partitionsList,
                                                           final String partition) {
        return partitionsList
                .stream()
                .filter(pMap -> partition.equals(pMap.get("partition")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("partition not found"));
    }

    private void validateOffsetIncreasedBy(final Map<String, String> partitionInfoBefore,
                                           final Map<String, String> partitionInfoAfter, final long delta) {
        final long offsetBefore = Long.parseLong(partitionInfoBefore.get("newestAvailableOffset"));
        final long offsetAfter = Long.parseLong(partitionInfoAfter.get("newestAvailableOffset"));
        assertThat(offsetAfter, is(offsetBefore + delta));
    }

    private void writeMessageToPartition(final int partition) throws InterruptedException, ExecutionException {
        final KafkaProducer<String, String> producer = kafkaHelper.createProducer();
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, partition, "blahKey",
                "blahValue");
        producer.send(producerRecord).get();
    }

    private void validatePartitionStructure(final Map<String, String> pMap) {
        assertThat(pMap.get("partition"), Matchers.notNullValue());
        assertThat(pMap.get("newestAvailableOffset"), Matchers.notNullValue());
        assertThat(pMap.get("oldestAvailableOffset"), Matchers.notNullValue());
    }

    private List<Map<String, String>> asMapsList(final String body) throws IOException {
        return jsonMapper.<List<Map<String, String>>>readValue(body, LIST_OF_MAPS_REF);
    }

    private Map<String, String> asMap(final String body) throws IOException {
        return jsonMapper.<Map<String, String>>readValue(body, MAP_REF);
    }
}
