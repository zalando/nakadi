package de.zalando.aruha.nakadi.webservice;

import com.jayway.restassured.RestAssured;
import de.zalando.aruha.nakadi.Application;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.webservice.utils.ATUtils;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.List;

import static com.jayway.restassured.RestAssured.given;
import static de.zalando.aruha.nakadi.webservice.utils.ATUtils.getLatestOffsets;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;

public class EventStreamReadingAT {

    private static final String TEST_TOPIC = "test-topic";
    private static final String PARTITION = "0";
    private static final String EVENTS_ENDPOINT = "/topics/" + TEST_TOPIC + "/partition/" + PARTITION + "/events";

    @Before
    public void setUp() {
        RestAssured.port = 8080;

//        RestAssured.given().when().post(EVENTS_ENDPOINT);
    }

    @Test
    public void whenGetSingleBatchThenOk() {
        final List latestOffsets = getLatestOffsets(TEST_TOPIC);
        assertNotNull(latestOffsets);
//        given()
//                .param("start_from", "0")
//        .when()
//                .get(EVENTS_ENDPOINT)
//        .then()
//                .statusCode(HttpStatus.SC_OK)
//                .body(is(echo));
    }
}
