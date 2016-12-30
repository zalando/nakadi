package org.zalando.nakadi.domain;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BatchItemTest {

    @Test
    public void testBatchItemSizeWithMultByteChar() {
        final BatchItem item = new BatchItem("{ \"name\": \"香港\"} ");
        assertEquals(20, item.getEventSize());
    }
}
