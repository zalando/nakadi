package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;
import static org.zalando.nakadi.webservice.BaseAT.TIMELINE_REPOSITORY;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.postEvents;

public class CursorOperationsAT {
    private static EventType eventType = EventTypeTestBuilder.builder().build();
    private static final String EVENT = "{\"foo\":\"" + randomTextString() + "\"}";

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
    public void calculateCursorLagForMultipleTimelines() {
        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("BEGIN"))
                .body("oldest_available_offset[0]", equalTo("000000000000000000"))
                .body("unconsumed_events[0]", equalTo(0));

        postEvents(eventType.getName(), EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("000000000000000000"))
                .body("unconsumed_events[0]", equalTo(2));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0001-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(2));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0001-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(2));

        postEvents(eventType.getName(), EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0002-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(4));

        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0002-000000000000000001"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(4));

        postEvents(eventType.getName(), EVENT, EVENT, EVENT);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body("[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]")
                .when()
                .post("/event-types/" + eventType.getName() + "/cursors-lag")
                .then()
                .statusCode(OK.value())
                .body("size()", equalTo(1))
                .body("newest_available_offset[0]", equalTo("001-0003-000000000000000002"))
                .body("oldest_available_offset[0]", equalTo("001-0001-000000000000000000"))
                .body("unconsumed_events[0]", equalTo(7));
    }
}
