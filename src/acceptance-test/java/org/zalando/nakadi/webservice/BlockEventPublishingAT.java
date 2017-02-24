package org.zalando.nakadi.webservice;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.BlacklistService;

import java.io.IOException;
import java.text.MessageFormat;

import static com.jayway.restassured.RestAssured.given;
import static org.zalando.nakadi.utils.TestUtils.waitFor;

public class BlockEventPublishingAT extends BaseAT {

    @Test
    public void whenPublishingToBlockedEventTypeThen403() throws IOException {
        publishEvent(EVENT_TYPE)
                .then()
                .statusCode(HttpStatus.SC_OK);

        SettingsControllerAT.blacklist(EVENT_TYPE.getName(), BlacklistService.Type.PRODUCER_ET);

        waitFor(() -> publishEvent(EVENT_TYPE)
                .then()
                .statusCode(403)
                .body("detail", Matchers.equalTo("Application or event type is blocked")));

        SettingsControllerAT.whitelist(EVENT_TYPE.getName(), BlacklistService.Type.PRODUCER_ET);

        waitFor(() -> publishEvent(EVENT_TYPE)
                .then()
                .statusCode(HttpStatus.SC_OK));
    }

    private Response publishEvent(final EventType eventType) {
        return given()
                .body("[{\"foo\":\"bar\"}]")
                .contentType(ContentType.JSON)
                .post(MessageFormat.format("/event-types/{0}/events", eventType.getName()));
    }

}