package org.zalando.nakadi.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;

import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


public class BatchItemResponseTest {

    private ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = (new JsonConfig()).jacksonObjectMapper();
    }

    @Test
    public void testEnumsHaveLowerCaseSerialization() throws Exception {

        final String detail = "detail";
        final String eid = UUID.randomUUID().toString();
        final TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {
        };
        for (final EventPublishingStatus status : EventPublishingStatus.values()) {
            for (final EventPublishingStep step : EventPublishingStep.values()) {
                final BatchItemResponse bir = new BatchItemResponse();
                bir.setStep(step);
                bir.setPublishingStatus(status);
                bir.setDetail(detail);
                bir.setEid(eid);
                final String json = mapper.writeValueAsString(bir);
                final Map<String, String> jsonMap = mapper.readValue(json, typeRef);
                assertEquals(status.name().toLowerCase(), jsonMap.get("publishing_status"));
                assertEquals(step.name().toLowerCase(), jsonMap.get("step"));
            }
        }
    }
}