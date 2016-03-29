package de.zalando.aruha.nakadi.domain;

public enum EventPublishingStep {
    NONE, VALIDATION, ENRICHMENT, PARTITIONING, PUBLISHING
}
