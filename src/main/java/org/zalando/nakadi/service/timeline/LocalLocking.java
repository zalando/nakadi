package org.zalando.nakadi.service.timeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class LocalLocking {
    private static final Logger LOG = LoggerFactory.getLogger(LocalLocking.class);
    private final Set<String> lockedEventTypes = new HashSet<>();
    private final Map<String, Integer> eventTypesBeingPublished = new HashMap<>();
    private final Object lock = new Object();

    public Closeable workWithEventType(final String eventType, final long timeoutMs)
            throws InterruptedException, TimeoutException {
        final long finishAt = System.currentTimeMillis() + timeoutMs;
        synchronized (lock) {
            long now = System.currentTimeMillis();
            while (now < finishAt && lockedEventTypes.contains(eventType)) {
                lock.wait(finishAt - now);
                now = System.currentTimeMillis();
            }
            if (lockedEventTypes.contains(eventType)) {
                throw new TimeoutException("Timed out while waiting for event type " + eventType +
                        " to unlock within " + timeoutMs + " ms");
            }
            eventTypesBeingPublished.put(eventType, eventTypesBeingPublished.getOrDefault(eventType, 0) + 1);
        }
        return () -> {
            synchronized (lock) {
                final int currentCount = eventTypesBeingPublished.get(eventType);
                if (1 == currentCount) {
                    eventTypesBeingPublished.remove(eventType);
                    lock.notifyAll();
                } else {
                    eventTypesBeingPublished.put(eventType, currentCount - 1);
                }
            }
        };
    }

    public Set<String> getUnlockedEventTypes(final Set<String> lockedEventTypesUpdated) {
        synchronized (lock) {
            return this.lockedEventTypes.stream()
                    .filter(v -> !lockedEventTypesUpdated.contains(v))
                    .collect(Collectors.toSet());
        }
    }

    public void updateLockedEventTypes(final Set<String> lockedEventTypes) throws InterruptedException {
        synchronized (lock) {
            this.lockedEventTypes.clear();
            this.lockedEventTypes.addAll(lockedEventTypes);
            boolean haveUsage = true;
            while (haveUsage) {
                final List<String> stillLocked = this.lockedEventTypes.stream()
                        .filter(eventTypesBeingPublished::containsKey).collect(Collectors.toList());
                haveUsage = !stillLocked.isEmpty();
                if (haveUsage) {
                    LOG.info("Event types are still locked: {}", stillLocked);
                    lock.wait();
                }
            }
            lock.notifyAll();
        }
    }
}
