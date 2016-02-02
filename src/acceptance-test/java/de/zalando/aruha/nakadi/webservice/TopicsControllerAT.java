package de.zalando.aruha.nakadi.webservice;

import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.webservice.utils.KafkaTestHelper;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.when;
import static de.zalando.aruha.nakadi.webservice.utils.JsonTestHelper.asMapsList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TopicsControllerAT extends BaseAT {

    private Map<String, List<PartitionInfo>> actualTopics;

    @Before
    public void setup() {
        final KafkaTestHelper kafkaHelper = new KafkaTestHelper(kafkaUrl);
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


}
