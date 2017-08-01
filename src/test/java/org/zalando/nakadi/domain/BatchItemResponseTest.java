package org.zalando.nakadi.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


public class BatchItemResponseTest {

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
                final String json = TestUtils.OBJECT_MAPPER.writeValueAsString(bir);
                final Map<String, String> jsonMap = TestUtils.OBJECT_MAPPER.readValue(json, typeRef);
                assertEquals(status.name().toLowerCase(), jsonMap.get("publishing_status"));
                assertEquals(step.name().toLowerCase(), jsonMap.get("step"));
            }
        }
    }
}
