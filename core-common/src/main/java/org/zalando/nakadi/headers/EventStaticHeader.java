package org.zalando.nakadi.headers;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.view.EventOwnerSelector;

public class EventStaticHeader implements EventHeader {
    public void prepare(final BatchItem item, final EventOwnerSelector selector) {
        item.setHeader(new EventOwnerHeader(selector.getName(), selector.getValue()));
    }
}
