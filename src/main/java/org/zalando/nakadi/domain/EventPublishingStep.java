package org.zalando.nakadi.domain;

public enum EventPublishingStep {
    NONE,
    VALIDATING,
    ENRICHING,
    VALIDATING_SIZE,
    PARTITIONING,
    PUBLISHING,
}
