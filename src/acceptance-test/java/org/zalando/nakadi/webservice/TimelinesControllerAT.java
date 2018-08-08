package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.view.TimelineView;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.jayway.restassured.http.ContentType.JSON;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.zalando.nakadi.util.DateWithinMatcher.dateWithin;
import static org.zalando.nakadi.webservice.BaseAT.MAPPER;

public class TimelinesControllerAT extends RealEnvironmentAT {

    private static final long RETENTION_TIME_MS = Duration.of(2, DAYS).toMillis();

    @Test
    public void testCreateNextTimeline() throws Exception {
        final EventTypeOptions eventTypeOptions = new EventTypeOptions();
        eventTypeOptions.setRetentionTime(RETENTION_TIME_MS);

        final EventType eventType = EventTypeTestBuilder.builder()
                .options(eventTypeOptions)
                .build();

        NakadiTestUtils.createEventTypeInNakadi(eventType);

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        RestAssured.given()
                .contentType(ContentType.JSON)
                .get("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("[0].event_type", equalTo(eventType.getName()))
                .body("[0].order", Matchers.is(1))
                .body("[0].storage_id", equalTo("default"))
                .body("[0].switched_at", dateWithin(1, MINUTES, new DateTime()))
                .body("[0].cleaned_up_at", dateWithin(1, MINUTES, new DateTime().plusMillis((int) RETENTION_TIME_MS)))
                .body("[1].event_type", equalTo(eventType.getName()))
                .body("[1].order", Matchers.is(2))
                .body("[1].storage_id", equalTo("default"))
                .body("[1].switched_at", dateWithin(1, MINUTES, new DateTime()))
                .body("[1].cleaned_up_at", nullValue());
    }

    @Test
    public void testCreatTimelineOnExistentTopic() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .category(EventCategory.BUSINESS)
                .enrichmentStrategies(ImmutableList.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();

        NakadiTestUtils.createEventTypeInNakadi(eventType);

        final String existingTopic = "existing-topic";

        RestAssured.given()
                .body(new JSONObject().put("storage_id", "default").put("topic", existingTopic))
                .header("Content-type", "application/json")
                .post("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .body(equalTo(""))
                .statusCode(HttpStatus.SC_CREATED);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .get("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("[1].event_type", equalTo(eventType.getName()))
                .body("[1].order", Matchers.is(2))
                .body("[1].storage_id", equalTo("default"))
                .body("[1].topic", equalTo(existingTopic))
                .body("[1].cleaned_up_at", nullValue());
    }
}
