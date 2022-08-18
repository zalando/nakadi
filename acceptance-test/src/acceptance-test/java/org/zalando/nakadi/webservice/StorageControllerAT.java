package org.zalando.nakadi.webservice;

import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.junit.Assert.fail;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;

public class StorageControllerAT extends BaseAT {

// TODO: Ignored until fix the issue
    @Ignore
    @Test
    public void shouldChangeDefaultStorageWhenRequested() throws Exception {
        given()
                .body("{" +
                        "\"id\": \"default-test\"," +
                        "\"kafka_configuration\": {" +
                        "\"zookeeper_connection\":{" +
                        "\"type\": \"zookeeper\"," +
                        "\"addresses\":[{\"address\":\"zookeeper\", \"port\":2181}]" +
                        "}" +
                        "},\"storage_type\": \"kafka\"}")
                .contentType(JSON).post("/storages")
                .then()
                .statusCode(HttpStatus.SC_CREATED);

        NakadiTestUtils.createEventTypeInNakadi(EventTypeTestBuilder.builder().name("event_a").build());
        final String storageId = (String) NakadiTestUtils.listTimelines("event_a").get(0).get("storage_id");
        Assert.assertEquals("default", storageId);

        given().contentType(JSON).put("/storages/default/default-test")
                .then()
                .statusCode(HttpStatus.SC_OK);

        // change of default storage is eventually consistent and relies on zookeeper watcher being processed
        waitFor(() -> {
            try {
                final EventType et = createEventType();
                final String storage = (String) NakadiTestUtils.listTimelines(et.getName()).get(0).get("storage_id");
                Assert.assertEquals("default-test", storage);
            } catch (final Exception e) {
                fail(e.getMessage());
            }
        });
    }

    @After
    public void cleanUp() {
        given().contentType(JSON).put("/storages/default/default");
    }
}
