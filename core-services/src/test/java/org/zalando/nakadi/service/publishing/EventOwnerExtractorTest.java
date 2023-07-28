package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.StrictJsonParser;
import org.zalando.nakadi.exceptions.runtime.EventOwnerExtractionException;

import java.io.IOException;

public class EventOwnerExtractorTest {
    private static final JSONObject MOCK_EVENT = StrictJsonParser.parse("{" +
            "\"other\": null, \n" +
            "\"example\": {\n" +
            "\"security\": {\"final\": \"test_value\"}}" +
            "}", false);

    @Test
    public void testCorrectValueProjectedWhenNestedEventWithPathValue() {
        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createPathExtractor(
                "retailer_id", "example.security.final");

        final EventOwnerHeader result = extractor.extractEventOwner(MOCK_EVENT);
        Assert.assertEquals(new EventOwnerHeader("retailer_id", "test_value"), result);
    }

    @Test
    public void testAbsenceOfPathValueIsAnError() {
        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createPathExtractor(
                "retailer_id", "example.nothing.here");

        Assert.assertThrows(EventOwnerExtractionException.class, () -> extractor.extractEventOwner(MOCK_EVENT));
    }

    @Test
    public void testNullWithPathValueIsAnError() {
        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createPathExtractor(
                "retailer_id", "other");

        Assert.assertThrows(EventOwnerExtractionException.class, () -> extractor.extractEventOwner(MOCK_EVENT));
    }

    @Test
    public void testCorrectValueSetWhenStatic() {
        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createStaticExtractor(
                "retailer_id", "examplexx");

        final EventOwnerHeader result = extractor.extractEventOwner(MOCK_EVENT);
        Assert.assertEquals(new EventOwnerHeader("retailer_id", "examplexx"), result);
    }

    @Test
    public void testCorrectValueSetWhenMetadata() throws IOException {
        final NakadiMetadata metadata = new NakadiMetadata();
        metadata.setEventOwner("owner-123");

        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createMetadataExtractor("retailer_id");

        final EventOwnerHeader result = extractor.extractEventOwner(metadata);
        Assert.assertEquals(new EventOwnerHeader("retailer_id", "owner-123"), result);
    }
}
