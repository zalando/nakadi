package org.zalando.nakadi.domain;

public enum EventPublishingStep {
    NONE,
    VALIDATING,
    ENRICHING,
    PARTITIONING,
    PUBLISHING,
    PUBLISHED,
    FAILED_PUBLISH
}
