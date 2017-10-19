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

    private static JSONObject restoreJsonObject(final BatchItem bi) {
        final String result = bi.dumpEventToString();
        Assert.assertEquals(-1, result.indexOf('\r'));
        Assert.assertEquals(-1, result.indexOf('\n'));
        return new JSONObject(result);
    }

    @Test
    public void testBatchItemSizeWithMultByteChar() {
        final BatchItem item = new BatchItem("{ \"name\": \"香港\"} ",
                BatchItem.EmptyInjectionConfiguration.build(1, false),
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length],
                Collections.emptyList());
        assertEquals(20, item.getEventSize());
    }

    @Test
    public void testBatchItemReplacementsWithMetadata() {
        final JSONObject event = new JSONObject();
        final JSONObject sourceMetadata = new JSONObject();
        sourceMetadata.put("eid", UUID.randomUUID().toString());
        sourceMetadata.put("occurred_at", (new DateTime(DateTimeZone.UTC)).toString());
        sourceMetadata.put("partition", "0");

        event.put("metadata", sourceMetadata);
        event.put("foo", "Test data data data");
        final BatchItem bi = BatchFactory.from("[" + event.toString(2) + "]").get(0);

        final JSONObject metadata = bi.getEvent().getJSONObject(BatchItem.Injection.METADATA.name);
        metadata.put("test_test_test", "test2");
        bi.inject(BatchItem.Injection.METADATA, metadata.toString());

        final JSONObject result = restoreJsonObject(bi);
        Assert.assertEquals("test2", result.getJSONObject("metadata").getString("test_test_test"));
        Assert.assertEquals("Test data data data", result.getString("foo"));
    }

    @Test
    public void testBatchItemReplacementsNoMetadata() {
        final JSONObject event = new JSONObject();
        event.put("foo", "Test data data data");
        final BatchItem bi = BatchFactory.from("[" + event.toString(2) + "]").get(0);

        final JSONObject metadata = new JSONObject();
        metadata.put("test_test_test", "test2");
        bi.inject(BatchItem.Injection.METADATA, metadata.toString());

        final JSONObject result = restoreJsonObject(bi);
        Assert.assertEquals("Test data data data", result.getString("foo"));
        Assert.assertEquals("test2", result.getJSONObject("metadata").getString("test_test_test"));
    }

    @Test
    public void testBatchReplacementEmptyEvent() {
        final BatchItem bi = BatchFactory.from("[{}]").get(0);

        final JSONObject metadata = new JSONObject();
        metadata.put("test_test_test", "test2");
        bi.inject(BatchItem.Injection.METADATA, metadata.toString());

        final JSONObject result = restoreJsonObject(bi);
        Assert.assertEquals("test2", result.getJSONObject("metadata").getString("test_test_test"));
    }

    @Test
    public void testNoReplacements() {
        final BatchItem bi = BatchFactory.from("[{\"test_test_test\"\n\r\n: \"test2\"}\n\t\r]").get(0);
        final JSONObject result = restoreJsonObject(bi);
        Assert.assertEquals("test2", result.getString("test_test_test"));
    }

    @Test
    public void testNoReplacementsEmpty() {
        final BatchItem bi = BatchFactory.from("[{\n\n\n\n\n\n\n\n}]").get(0);
        final JSONObject result = restoreJsonObject(bi);
        Assert.assertFalse(result.keys().hasNext());
    }
}
