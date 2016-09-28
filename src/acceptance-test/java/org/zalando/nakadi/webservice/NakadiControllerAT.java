package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.http.ContentType;
import org.apache.commons.collections.map.HashedMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.FloodService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import static com.jayway.restassured.RestAssured.given;

public class NakadiControllerAT extends BaseAT {

    private static final String FLOODERS_URL = "/nakadi/flooders";
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    @Test
    public void testBlockFlooder() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();

        final FloodService.Flooder flooder =
                new FloodService.Flooder(eventType.getName(), FloodService.Type.CONSUMER_ET);
        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .post(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        Assert.assertNotNull(CURATOR.checkExists()
                .forPath("/nakadi/flooders/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testUnBlockFlooder() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        final FloodService.Flooder flooder =
                new FloodService.Flooder(eventType.getName(), FloodService.Type.CONSUMER_ET);

        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .post(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .delete(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        Assert.assertNull(CURATOR.checkExists()
                .forPath("/nakadi/flooders/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testGetFlooders() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();

        final FloodService.Flooder flooder =
                new FloodService.Flooder(eventType.getName(), FloodService.Type.CONSUMER_ET);
        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .post(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        given()
                .contentType(ContentType.JSON)
                .get(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_OK)
                .content(JSON_HELPER.matchesObject(new HashedMap() {
                    {
                        put("consumers", new HashMap<String, Set<String>>() {{
                            put("event_types", Collections.singleton(eventType.getName()));
                            put("apps", Collections.emptySet());
                        }});
                        put("producers", new HashMap<String, Set<String>>() {{
                            put("event_types", Collections.emptySet());
                            put("apps", Collections.emptySet());
                        }});
                    }
                }));
    }

}