package org.zalando.nakadi.domain;

public enum CompatibilityMode {
    FORWARD, COMPATIBLE, NONE,
    BACKWARD, BACKWARD_TRANSITIVE,
    FORWARD_TRANSITIVE, COMPATIBLE_TRANSITIVE
}
