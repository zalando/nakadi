package org.zalando.nakadi.headers;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.util.JsonPathAccess;
import org.zalando.nakadi.view.EventOwnerSelector;

public class EventPathHeaderHandler implements EventHeaderHandler {
    public void prepare(final BatchItem item, final EventOwnerSelector selector) {
        final JsonPathAccess jsonPath = new JsonPathAccess(item.getEvent());
        final String value = jsonPath.get(selector.getValue()).toString();
        item.setHeader(new EventOwnerHeader(selector.getName(), value));
    }
}
