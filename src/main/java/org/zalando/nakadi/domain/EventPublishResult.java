package org.zalando.nakadi.domain;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class EventPublishResult {
    private final EventPublishingStatus status;
    private final EventPublishingStep step;
    private final List<BatchItemResponse> responses;
}
