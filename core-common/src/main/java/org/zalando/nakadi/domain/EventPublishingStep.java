package org.zalando.nakadi.domain;

public enum EventPublishingStep {
    NONE,
    VALIDATING,
    ENRICHING,
    PARTITIONING,
    AUTHORIZING,
    PUBLISHING,
}
