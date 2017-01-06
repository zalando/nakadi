package org.zalando.nakadi.service.timeline;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalLocking {
    private static final Logger LOG = LoggerFactory.getLogger(LocalLocking.class);
    private final Set<String> lockedEventTypes = new HashSet<>();
    private final Map<String, Integer> eventsBeingPublished = new HashMap<>();
    private final Object lock = new Object();

    public Set<String> lockedEventTypesChanged(final Set<String> lockedEventTypes) throws InterruptedException {
        final Set<String> unlockedEventTypes = new HashSet<>();
        synchronized (lock) {
            for (final String item : this.lockedEventTypes) {
                if (!lockedEventTypes.contains(item)) {
                    unlockedEventTypes.add(item);
                }
            }
            this.lockedEventTypes.clear();
            this.lockedEventTypes.addAll(lockedEventTypes);
            boolean haveUsage = true;
            while (haveUsage) {
                final List<String> stillLocked = this.lockedEventTypes.stream()
                        .filter(eventsBeingPublished::containsKey).collect(Collectors.toList());
                haveUsage = !stillLocked.isEmpty();
                if (haveUsage) {
                    LOG.info("Event types are still locked: {}", stillLocked);
                    lock.wait();
                }
            }
            lock.notifyAll();
        }
        return unlockedEventTypes;
    }

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
            eventsBeingPublished.put(eventType, eventsBeingPublished.getOrDefault(eventType, 0) + 1);
        }
        return () -> {
            synchronized (lock) {
                final int currentCount = eventsBeingPublished.get(eventType);
                if (1 == currentCount) {
                    eventsBeingPublished.remove(eventType);
                    lock.notifyAll();
                } else {
                    eventsBeingPublished.put(eventType, currentCount - 1);
                }
            }
        };
    }
}
