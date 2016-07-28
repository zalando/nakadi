package org.zalando.nakadi.domain;

public enum CursorError {
    PARTITION_NOT_FOUND, EMPTY_PARTITION, UNAVAILABLE, INVALID_FORMAT, NULL_PARTITION, NULL_OFFSET
}
