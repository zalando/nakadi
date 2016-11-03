package org.zalando.nakadi.webservice;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.problem.MoreStatus;

import java.io.IOException;
import java.text.MessageFormat;

import static com.jayway.restassured.RestAssured.given;

public class BlockEventPublishingAT extends BaseAT {

    @Test
    public void whenPublishingToBlockedEventTypeThen429() throws IOException {
        final EventType eventType = NakadiTestUtils.createEventType();

        publishEvent(eventType)
                .then()
                .statusCode(HttpStatus.SC_OK);

        final BlacklistService.Flooder flooder =
                new BlacklistService.Flooder(eventType.getName(), BlacklistService.Type.PRODUCER_ET);
        SettingsControllerAT.blockFlooder(flooder);
        publishEvent(eventType)
                .then()
                .statusCode(MoreStatus.TOO_MANY_REQUESTS.getStatusCode())
                .header("Retry-After", "300");

        SettingsControllerAT.unblockFlooder(flooder);
        publishEvent(eventType)
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    private Response publishEvent(final EventType eventType) {
        return given()
                .body("[{\"blah\":\"bloh\"}]")
                .contentType(ContentType.JSON)
                .post(MessageFormat.format("/event-types/{0}/events", eventType.getName()));
    }

}