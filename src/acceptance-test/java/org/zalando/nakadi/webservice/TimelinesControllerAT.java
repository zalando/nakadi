package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

public class TimelinesControllerAT extends BaseAT {

    private static EventType eventType = EventTypeTestBuilder.builder().build();

    @BeforeClass
    public static void setUp() throws JsonProcessingException {
        NakadiTestUtils.createEventTypeInNakadi(eventType);
    }

    @AfterClass
    public static void tearDown() {
        TIMELINE_REPOSITORY.listTimelinesOrdered(eventType.getName()).stream()
                .forEach(timeline -> TIMELINE_REPOSITORY.deleteTimeline(timeline.getId()));
        RestAssured.given().delete("/event-types/{name}", eventType.getName());
    }

    @Test
    public void testCreateTimelineFromFake() throws Exception {
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        RestAssured.given()
                .contentType(ContentType.JSON)
                .get("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("[0].event_type", Matchers.equalTo(eventType.getName()))
                .body("[0].order", Matchers.is(1))
                .body("[0].storage_id", Matchers.equalTo("default"));
    }

    @Test
    public void testCreateTimelineFromReal() throws Exception {
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        RestAssured.given()
                .contentType(ContentType.JSON)
                .get("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("[0].event_type", Matchers.equalTo(eventType.getName()))
                .body("[0].order", Matchers.is(1))
                .body("[0].storage_id", Matchers.equalTo("default"))
                .body("[1].event_type", Matchers.equalTo(eventType.getName()))
                .body("[1].order", Matchers.is(2))
                .body("[1].storage_id", Matchers.equalTo("default"));
    }

    @Test
    public void testDeleteTimelineWhenOnlyOneTimeline() throws Exception {
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        final String uuid = RestAssured.given()
                .contentType(ContentType.JSON)
                .get("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .extract().body().path("[0].id");

        RestAssured.given()
                .contentType(ContentType.JSON)
                .delete("event-types/{et_name}/timelines/{uuid}", eventType.getName(), uuid)
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void testDeleteTimelineWhenMoreThanOneTimelineThenError() throws Exception {
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        final String uuid = RestAssured.given()
                .contentType(ContentType.JSON)
                .get("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .extract().body().path("[0].id");

        RestAssured.given()
                .contentType(ContentType.JSON)
                .delete("event-types/{et_name}/timelines/{uuid}", eventType.getName(), uuid)
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
                .body("detail", Matchers.stringContainsInOrder(ImmutableList.of("Timeline with id:",
                        "could not be deleted. It is possible to delete a timeline if there is only one timeline")));
    }

}