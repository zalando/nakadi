package org.zalando.nakadi.webservice;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import java.text.MessageFormat;

import static com.jayway.restassured.RestAssured.given;

public class EventPublishingAT extends BaseAT {

    @Test
    public void whenPublishingEventTooLargeThen422() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();

        publishLargeEvent(eventType)
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    private Response publishLargeEvent(final EventType eventType) {
        StringBuilder sb = new StringBuilder();
        sb.append("[{\"blah\":\"");
        for (int i = 0; i < 1000010; i++) {
            sb.append("a");
        }
        sb.append("\"}]");
        return given()
                .body(sb.toString())
                .contentType(ContentType.JSON)
                .post(MessageFormat.format("/event-types/{0}/events", eventType.getName()));
    }
}
