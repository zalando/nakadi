package org.zalando.nakadi.headers;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.view.EventOwnerSelector;

public class EventPathHeaderHandlerTest {
    private final EventPathHeaderHandler headerHandler = new EventPathHeaderHandler();

    private String getMockEvent() {
        return "{" +
                "\"example\": {\n" +
                "    \"security\": {\"final\": \"test_value\"}}" +
                "}";
    }

    @Test
    public void correctValueProjectedWhenNestedEventWithPathValue() {
        final EventOwnerSelector selector = new EventOwnerSelector(
                EventOwnerSelector.Type.PATH, "retailer_id", "example.security.final");

        final BatchItem item = new BatchItem(getMockEvent(), null, null, null);
        headerHandler.prepare(item, selector);

        Assert.assertTrue(item.getHeader().isPresent());
        Assert.assertEquals(item.getHeader().get().getValue(), "test_value");
    }
}
