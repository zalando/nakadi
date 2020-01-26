package org.zalando.nakadi.headers;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.view.EventOwnerSelector;

public class EventHeaderHandlerTest {
    private final EventPathHeaderHandler pathHeaderHandler = new EventPathHeaderHandler();
    private final EventStaticHeaderHandler staticHeaderHandler = new EventStaticHeaderHandler();

    private String getMockEvent() {
        return "{" +
                "\"example\": {\n" +
                "    \"security\": {\"final\": \"test_value\"}}" +
                "}";
    }

    @Test
    public void testCorrectValueProjectedWhenNestedEventWithPathValue() {
        final EventOwnerSelector selector = new EventOwnerSelector(
                EventOwnerSelector.Type.PATH, "retailer_id", "example.security.final");

        final BatchItem item = new BatchItem(getMockEvent(), null, null, null);
        pathHeaderHandler.prepare(item, selector);

        Assert.assertTrue(item.getHeader().isPresent());
        Assert.assertEquals(item.getHeader().get().getValue(), "test_value");
    }

    @Test
    public void testCorrectValueSetWhenStaticPath() {
        final EventOwnerSelector selector = new EventOwnerSelector(
                EventOwnerSelector.Type.STATIC, "retailer_id", "example");

        final BatchItem item = new BatchItem(getMockEvent(), null, null, null);
        staticHeaderHandler.prepare(item, selector);

        Assert.assertTrue(item.getHeader().isPresent());
        Assert.assertEquals(item.getHeader().get().getValue(), "example");
    }
}
