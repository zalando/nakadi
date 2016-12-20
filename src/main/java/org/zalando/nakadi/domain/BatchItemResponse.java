package org.zalando.nakadi.domain;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BatchItemResponse {
    private volatile EventPublishingStatus publishingStatus = EventPublishingStatus.ABORTED;
    private volatile String detail = "";
    private EventPublishingStep step = EventPublishingStep.NONE;
    private String eid = "";
}
