package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.http.ContentType;
import org.apache.commons.collections.map.HashedMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.FloodService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import static com.jayway.restassured.RestAssured.given;

public class NakadiControllerAT extends BaseAT {

    private static final String FLOODERS_URL = "/nakadi/flooders";
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final JsonTestHelper JSON_HELPER = new JsonTestHelper(MAPPER);
    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);

    @Before
    public void setUp() throws Exception{
        clearFloodersData();
    }

    @Test
    public void testBlockFlooder() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        blockFlooder(new FloodService.Flooder(eventType.getName(), FloodService.Type.CONSUMER_ET));

        Assert.assertNotNull(CURATOR.checkExists()
                .forPath("/nakadi/flooders/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testUnblockFlooder() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        final FloodService.Flooder flooder =
                new FloodService.Flooder(eventType.getName(), FloodService.Type.CONSUMER_ET);
        blockFlooder(flooder);

        unblockFlooder(flooder);

        Assert.assertNull(CURATOR.checkExists()
                .forPath("/nakadi/flooders/consumers/event_types/" + eventType.getName()));
    }

    @Test
    public void testGetFlooders() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();

        final FloodService.Flooder flooder =
                new FloodService.Flooder(eventType.getName(), FloodService.Type.CONSUMER_ET);
        blockFlooder(flooder);

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

    private void clearFloodersData() throws Exception {
        CURATOR.delete().deletingChildrenIfNeeded().forPath("/nakadi/flooders");
    }

    public static void blockFlooder(final FloodService.Flooder flooder) throws IOException {
        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .post("/nakadi/flooders")
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    public static void unblockFlooder(FloodService.Flooder flooder) throws JsonProcessingException {
        given()
                .body(MAPPER.writeValueAsString(flooder))
                .contentType(ContentType.JSON)
                .delete(FLOODERS_URL)
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

}