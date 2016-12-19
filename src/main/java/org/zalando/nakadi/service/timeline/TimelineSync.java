package org.zalando.nakadi.service.timeline;

import java.io.Closeable;

/**
 * Interface for timeline locking.
 * Nodes looks like:
 * <pre>
 * - timelines
 *  + - lock
 *  + - version: monotonically_incremented long value
 *  + - locked_et: [et_1, et_2] - locked event types
 *  + - nodes - nakadi nodes
 *    + - {node_name}: {version: version_used} - node-specific information
 * </pre>
 */
public interface TimelineSync {
    public Closeable workWithEventType(String eventType);

    public void lockEventType(String eventType, long timeoutMs);

    public void unlockEventType(String eventType);
}
