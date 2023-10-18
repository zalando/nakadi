package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.hamcrest.Matchers.nullValue;
import static org.zalando.nakadi.util.DateWithinMatcher.dateWithin;

public class TimelinesControllerAT extends BaseAT {

    private static final long RETENTION_TIME_MS = Duration.of(2, DAYS).toMillis();

    private final EventType eventType;

    public TimelinesControllerAT() throws JsonProcessingException {
        final EventTypeOptions eventTypeOptions = new EventTypeOptions();
        eventTypeOptions.setRetentionTime(RETENTION_TIME_MS);

        eventType = EventTypeTestBuilder.builder()
                .options(eventTypeOptions)
                .build();

        NakadiTestUtils.createEventTypeInNakadi(eventType);
    }

    @Test
    public void testCreateNextTimeline() throws Exception {
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        RestAssured.given()
                .contentType(ContentType.JSON)
                .get("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("[0].event_type", Matchers.equalTo(eventType.getName()))
                .body("[0].order", Matchers.is(1))
                .body("[0].storage_id", Matchers.equalTo("default"))
                .body("[0].switched_at", dateWithin(1, MINUTES, new DateTime()))
                .body("[0].cleaned_up_at", dateWithin(1, MINUTES, new DateTime().plusMillis((int) RETENTION_TIME_MS)))
                .body("[1].event_type", Matchers.equalTo(eventType.getName()))
                .body("[1].order", Matchers.is(2))
                .body("[1].storage_id", Matchers.equalTo("default"))
                .body("[1].switched_at", dateWithin(1, MINUTES, new DateTime()))
                .body("[1].cleaned_up_at", nullValue());
    }

}
