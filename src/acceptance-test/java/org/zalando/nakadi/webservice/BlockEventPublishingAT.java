package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import org.apache.http.HttpStatus;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.FloodService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.problem.MoreStatus;

import java.io.IOException;
import java.text.MessageFormat;

import static com.jayway.restassured.RestAssured.given;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;

public class BlockEventPublishingAT extends BaseAT {

    private static final String FLOODERS_URL = "/settings/flooders";
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);

    @Test
    public void whenPublishingToBlockedEventTypeThen429() throws IOException {
        final EventType eventType = NakadiTestUtils.createEventType();

        publishEvent(eventType)
                .then()
                .statusCode(HttpStatus.SC_OK);

        final FloodService.Flooder flooder =
                new FloodService.Flooder(eventType.getName(), FloodService.Type.PRODUCER_ET);
        SettingsControllerAT.blockFlooder(flooder);

        waitForBlock(eventType);

        publishEvent(eventType)
                .then()
                .statusCode(MoreStatus.TOO_MANY_REQUESTS.getStatusCode())
                .header("Retry-After", "300");

        SettingsControllerAT.unblockFlooder(flooder);

        waitForUnblock(eventType);

        publishEvent(eventType)
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    private void waitForUnblock(final EventType eventType) {
        executeWithRetry(() -> {
            return !getFlooders().getJSONObject("producers").getJSONArray("event_types").toList()
                    .contains(eventType.getName());
        }, new RetryForSpecifiedTimeStrategy<Boolean>(5000).withResultsThatForceRetry(false).withWaitBetweenEachTry(
                500));
    }

    private void waitForBlock(final EventType eventType) {
        executeWithRetry(() -> {
            return getFlooders().getJSONObject("producers").getJSONArray("event_types").toList()
                            .contains(eventType.getName());
        }, new RetryForSpecifiedTimeStrategy<Boolean>(5000).withResultsThatForceRetry(false).withWaitBetweenEachTry(
                        500));
    }

    private JSONObject getFlooders() {
        return new JSONObject(given()
                                .header("accept", "application/json").get(FLOODERS_URL)
                                .getBody().asString());
    }

    private Response publishEvent(final EventType eventType) {
        return given()
                .body("[{\"blah\":\"bloh\"}]")
                .contentType(ContentType.JSON)
                .post(MessageFormat.format("/event-types/{0}/events", eventType.getName()));
    }

}