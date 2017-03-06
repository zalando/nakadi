package org.zalando.nakadi.service.timeline;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Interface for timeline locking.
 * Nodes looks like:
 * <pre>
 * - timelines
 *  + - lock                    lock for timeline versions synchronization
 *  + - version: {version}      monotonically incremented long value (version of timelines configuration)
 *  + - locked_et:
 *     | - et_1                 Event types that are paused right now (et_1)
 *     | - et_2                 The same goes here (et_2)
 *  + - nodes                   nakadi nodes
 *    + - {node1}: {version}    Each nakadi node exposes version being used on this node
 * </pre>
 */
public interface TimelineSync {
    /**
     * Call while publishing to event type. When publishing complete - call {@link Closeable#close()}
     *
     * @param eventType Event type to publish to
     * @param timeoutMs Timeout for operation
     * @return Closeable object, that should be closed when publishing complete
     * @throws TimeoutException In case when timeout passed and event type still wasn't unlocked
     */
    Closeable workWithEventType(String eventType, long timeoutMs) throws InterruptedException, TimeoutException;

    /**
     * Lock event type publishing while switching to next timeline
     *
     * @param eventType Event type to lock publishing to.
     * @param timeoutMs Timeout for sync operation.
     * @throws IllegalStateException In case when timeline update already started for specified event type
     */
    void startTimelineUpdate(String eventType, long timeoutMs)
            throws InterruptedException, IllegalStateException, RuntimeException;

    /**
     * Release publishing lock to event type
     *
     * @param eventType Event type to unlock publishing to.
     */
    void finishTimelineUpdate(String eventType) throws InterruptedException, RuntimeException;

    interface ListenerRegistration {
        void cancel();
    }

    /**
     * Register listener for timelines modification. It will be called when everyone is pretty sure that new timeline is
     * available in db and each node knows about it.
     *
     * @param eventType Event type to register listener for
     * @param listener  Listener that will accept event type.
     * @return Registration for listener. one should call cancel once registration is not needed anymore.
     */
    ListenerRegistration registerTimelineChangeListener(String eventType, Consumer<String> listener);
}
