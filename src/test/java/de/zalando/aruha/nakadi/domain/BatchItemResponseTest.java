package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.JsonConfig;
import org.junit.Before;
import org.junit.Test;

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

        String detail = "detail";
        String eid = UUID.randomUUID().toString();
        TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {
        };
        for (EventPublishingStatus status : EventPublishingStatus.values()) {
            for (EventPublishingStep step : EventPublishingStep.values()) {
                BatchItemResponse bir = new BatchItemResponse();
                bir.setStep(step);
                bir.setPublishingStatus(status);
                bir.setDetail(detail);
                bir.setEid(eid);
                String json = mapper.writeValueAsString(bir);
                Map<String, String> jsonMap = mapper.readValue(json, typeRef);
                assertEquals(status.name().toLowerCase(), jsonMap.get("publishing_status"));
                assertEquals(step.name().toLowerCase(), jsonMap.get("step"));
            }
        }
    }
}