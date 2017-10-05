package org.zalando.nakadi.domain;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class BatchItemTest {

    @Test
    public void testBatchItemSizeWithMultByteChar() {
        final BatchItem item = new BatchItem("{ \"name\": \"香港\"} ",
                BatchItem.EmptyInjectionConfiguration.build(1, false),
                Collections.emptyMap(),
                Collections.emptyList());
        assertEquals(20, item.getEventSize());
    }

    @Test
    public void testBatchItemReplacements() {
        final JSONObject event = new JSONObject();
        {
            final JSONObject metadata = new JSONObject();
            metadata.put("eid", UUID.randomUUID().toString());
            metadata.put("occurred_at", (new DateTime(DateTimeZone.UTC)).toString());
            metadata.put("partition", "0");

            event.put("metadata", metadata);
            event.put("foo", "Test data data data");
        }
        final BatchItem bi = BatchFactory.from("[" +event.toString(2) + "]").get(0);

        final JSONObject metadata = (JSONObject) bi.getEvent().get(BatchItem.Injection.METADATA.name);
        metadata.put("test_test_test", "test2");
        bi.inject(BatchItem.Injection.METADATA, metadata.toString());

        final String value = bi.dumpEventToString();

        Assert.assertNotNull(value);
    }
}
