package org.zalando.nakadi.headers;

import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.view.EventOwnerSelector;

public interface EventHeader {
    void prepare(BatchItem batchItem, EventOwnerSelector eventOwnerSelector);
}
