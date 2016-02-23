package de.zalando.aruha.nakadi.webservice;

import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTestHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.when;
import static de.zalando.aruha.nakadi.webservice.utils.JsonTestHelper.asMap;
import static de.zalando.aruha.nakadi.webservice.utils.JsonTestHelper.asMapsList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PartitionsControllerAT extends BaseAT {

    private KafkaTestHelper kafkaHelper;

    private Map<String, List<PartitionInfo>> actualTopics;

    @Before
    public void before() {
        kafkaHelper = new KafkaTestHelper(kafkaUrl);
        actualTopics = kafkaHelper.createConsumer().listTopics();
    }

    @Test
    public void whenListPartitionsThenOk() throws IOException {
        // ACT //
        final Response response = when().get(String.format("/event-types/%s/partitions", TEST_TOPIC));

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value());

        final List<Map<String, String>> partitionsList = asMapsList(response.print());
        partitionsList.forEach(this::validatePartitionStructure);

        final Set<String> partitions = partitionsList
                .stream()
                .map(map -> map.get("partition"))
                .collect(Collectors.toSet());
        final Set<String> actualPartitions = actualTopics
                .get(TEST_TOPIC)
                .stream()
                .map(pInfo -> Integer.toString(pInfo.partition()))
                .collect(Collectors.toSet());
        assertThat(partitions, equalTo(actualPartitions));
    }

    @Test
    public void whenListPartitionsThenTopicNotFound() throws IOException {
        when()
                .get("/event-types/not-existing-topic/partitions")
                .then()
                .statusCode(HttpStatus.NOT_FOUND.value())
                .and()
                .body("detail", equalTo("topic not found"));
    }

    @Test
    public void whenListPartitionsAndWriteMessageThenOffsetInPartitionIsIncreased() throws ExecutionException,
            InterruptedException, IOException {
        // ACT //
        final String url = String.format("/event-types/%s/partitions", TEST_TOPIC);
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
        final Response response = when().get(String.format("/event-types/%s/partitions/0", TEST_TOPIC));

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value());
        validatePartitionStructure(asMap(response.print()));
    }

    @Test
    public void whenGetPartitionThenTopicNotFound() throws IOException {
        when()
                .get("/event-types/not-existing-topic/partitions/0")
                .then()
                .statusCode(HttpStatus.NOT_FOUND.value())
                .and()
                .body("detail", equalTo("topic not found"));
    }

    @Test
    public void whenGetPartitionThenPartitionNotFound() throws IOException {
        when()
                .get(String.format("/event-types/%s/partitions/43766", TEST_TOPIC))
                .then()
                .statusCode(HttpStatus.NOT_FOUND.value())
                .and()
                .body("detail", equalTo("partition not found"));
    }

    @Test
    public void whenGetPartitionAndWriteMessageThenOffsetInPartitionIsIncreased() throws ExecutionException,
            InterruptedException, IOException {
        // ACT //
        final String url = String.format("/event-types/%s/partitions/0", TEST_TOPIC);
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
        final long offsetBefore = Long.parseLong(partitionInfoBefore.get("newest_available_offset"));
        final long offsetAfter = Long.parseLong(partitionInfoAfter.get("newest_available_offset"));
        assertThat(offsetAfter, is(offsetBefore + delta));
    }

    private void writeMessageToPartition(final int partition) throws InterruptedException, ExecutionException {
        final KafkaProducer<String, String> producer = kafkaHelper.createProducer();
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, partition, "blahKey",
                "blahValue");
        producer.send(producerRecord).get();
    }

    private void validatePartitionStructure(final Map<String, String> pMap) {
        assertThat(pMap.get("partition"), Matchers.notNullValue());
        assertThat(pMap.get("newest_available_offset"), Matchers.notNullValue());
        assertThat(pMap.get("oldest_available_offset"), Matchers.notNullValue());
    }

}
