package de.zalando.aruha.nakadi.domain;

public enum EventPublishingStep {
    NONE, VALIDATING, ENRICHING, PARTITIONING, PUBLISHING
}
