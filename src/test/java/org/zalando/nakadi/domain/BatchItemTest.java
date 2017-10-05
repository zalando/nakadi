package org.zalando.nakadi.domain;

import org.junit.Test;

import java.util.Collections;

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
}
