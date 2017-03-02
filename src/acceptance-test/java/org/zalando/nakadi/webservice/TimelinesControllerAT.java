package org.zalando.nakadi.webservice;

import com.google.common.collect.ImmutableList;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

public class TimelinesControllerAT extends BaseAT {

    @Test
    public void testCreateTimelineFromFake() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        postTimeline(eventType);
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
        final EventType eventType = NakadiTestUtils.createEventType();
        postTimeline(eventType);
        postTimeline(eventType);
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
        final EventType eventType = NakadiTestUtils.createEventType();
        postTimeline(eventType);
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
        final EventType eventType = NakadiTestUtils.createEventType();
        postTimeline(eventType);
        postTimeline(eventType);
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

    private void postTimeline(final EventType eventType) {
        RestAssured.given()
                .contentType(ContentType.JSON)
                .body(new JSONObject().put("storage_id", "default"))
                .post("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_CREATED);
    }

}