package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.webservice.utils.KafkaHelper;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TopicsControllerAT extends BaseAT {

    private static final TypeReference<ArrayList<HashMap<String, String>>> LIST_OF_MAPS_REF =
            new TypeReference<ArrayList<HashMap<String, String>>>() {};

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
        final String url = String.format("/topics/%s/partitions", TOPIC);
        final Response response = when().get(url);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value());

        final List<Map<String, String>> partitionsList = asMapsList(response.print());
        assertThat(partitionsList, hasSize(PARTITIONS_NUM));
    }

    private List<Map<String, String>> asMapsList(final String body) throws IOException {
        return jsonMapper.<List<Map<String, String>>>readValue(body, LIST_OF_MAPS_REF);
    }
}
