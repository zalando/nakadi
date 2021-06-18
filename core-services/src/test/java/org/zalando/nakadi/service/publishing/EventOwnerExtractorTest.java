package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.StrictJsonParser;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.function.Function;

public class EventOwnerExtractorTest {
    private static final JSONObject MOCK_EVENT = StrictJsonParser.parse("{" +
            "\"other\": null, \n" +
            "\"example\": {\n" +
                "\"security\": {\"final\": \"test_value\"}}" +
            "}", false);

    @Test
    public void testCorrectValueProjectedWhenNestedEventWithPathValue() {
        final EventOwnerSelector selector = new EventOwnerSelector(
                EventOwnerSelector.Type.PATH, "retailer_id", "example.security.final");
        final Function<JSONObject, EventOwnerHeader> extractor =
                EventOwnerExtractorFactory.createPathExtractor(selector);

        final EventOwnerHeader result = extractor.apply(MOCK_EVENT);
        Assert.assertEquals(new EventOwnerHeader("retailer_id", "test_value"), result);
    }

    @Test
    public void testAbsenceOfPathValue() {
        final Function<JSONObject, EventOwnerHeader> extractor = EventOwnerExtractorFactory.createPathExtractor(
                new EventOwnerSelector(EventOwnerSelector.Type.PATH, "retailer_id", "example.nothing.here"));

        final EventOwnerHeader result = extractor.apply(MOCK_EVENT);
        Assert.assertNull(result);
    }

    @Test
    public void testNullWithPathValue() {
        final Function<JSONObject, EventOwnerHeader> extractor = EventOwnerExtractorFactory.createPathExtractor(
                new EventOwnerSelector(EventOwnerSelector.Type.PATH, "retailer_id", "other"));

        final EventOwnerHeader result = extractor.apply(MOCK_EVENT);
        Assert.assertNull(result);
    }

    @Test
    public void testCorrectValueSetWhenStaticPath() {
        final Function<JSONObject, EventOwnerHeader> extractor = EventOwnerExtractorFactory.createStaticExtractor(
                new EventOwnerSelector(EventOwnerSelector.Type.STATIC, "retailer_id", "examplexx"));

        final EventOwnerHeader result = extractor.apply(MOCK_EVENT);
        Assert.assertEquals(new EventOwnerHeader("retailer_id", "examplexx"), result);
    }
}
