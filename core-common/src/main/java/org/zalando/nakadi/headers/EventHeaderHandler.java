package org.zalando.nakadi.headers;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.view.EventOwnerSelector;

public interface EventHeaderHandler {
    void prepare(BatchItem batchItem, EventOwnerSelector eventOwnerSelector);
}
