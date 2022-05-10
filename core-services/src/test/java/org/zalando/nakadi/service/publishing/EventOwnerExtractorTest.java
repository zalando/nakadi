package org.zalando.nakadi.service.publishing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.GenericRecordMetadata;
import org.zalando.nakadi.domain.StrictJsonParser;
import org.zalando.nakadi.domain.VersionedAvroSchema;
import org.zalando.nakadi.service.AvroSchema;

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
    public void testAbsenceOfPathValue() {
        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createPathExtractor(
                "retailer_id", "example.nothing.here");

        final EventOwnerHeader result = extractor.extractEventOwner(MOCK_EVENT);
        Assert.assertNull(result);
    }

    @Test
    public void testNullWithPathValue() {
        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createPathExtractor(
                "retailer_id", "other");

        final EventOwnerHeader result = extractor.extractEventOwner(MOCK_EVENT);
        Assert.assertNull(result);
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
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        final AvroSchema avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
        final VersionedAvroSchema latestMeta = avroSchema.getLatestEventTypeSchemaVersion(AvroSchema.METADATA_KEY);

        final GenericRecordMetadata metadata = new GenericRecordMetadata(
                new GenericRecordBuilder(latestMeta.getSchema())
                        .set("occurred_at", System.currentTimeMillis())
                        .set("eid", "777bf536-ca07-4f23-abd1-16a30dc8f296")
                        .set("event_type", "doesn't-matter")
                        .set("version", "0")
                        .set(GenericRecordMetadata.EVENT_OWNER, "owner-123")
                        .build(),
                Byte.parseByte(latestMeta.getVersion()));

        final EventOwnerExtractor extractor = EventOwnerExtractorFactory.createMetadataExtractor("retailer_id");

        final EventOwnerHeader result = extractor.extractEventOwner(metadata);
        Assert.assertEquals(new EventOwnerHeader("retailer_id", "owner-123"), result);
    }
}
