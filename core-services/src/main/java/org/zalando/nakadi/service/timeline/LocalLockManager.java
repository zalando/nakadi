package org.zalando.nakadi.service.timeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@Service
public class LocalLockManager {
    private static final Logger LOG = LoggerFactory.getLogger(LocalLockManager.class);

    private final LocalLocking localLocking = new LocalLocking();
    private final List<Consumer<String>> headlessConsumerListeners = new ArrayList<>();
    private final Map<String, List<Consumer<String>>> consumerListeners = new HashMap<>();

    public void setLockedEventTypes(final Set<String> newList) throws InterruptedException {
        final Set<String> unlockedEventTypes = localLocking.getUnlockedEventTypes(newList);
        // Notify consumers that they should refresh timeline information
        for (final String unlocked : unlockedEventTypes) {
            LOG.info("Notifying about unlock of {}", unlocked);
            final List<Consumer<String>> toNotify;
            synchronized (headlessConsumerListeners) {
                toNotify = new ArrayList<>(headlessConsumerListeners);
            }
            synchronized (consumerListeners) {
                if (consumerListeners.containsKey(unlocked)) {
                    toNotify.addAll(consumerListeners.get(unlocked));
                }
            }
            for (final Consumer<String> listener : toNotify) {
                try {
                    listener.accept(unlocked);
                } catch (final RuntimeException ex) {
                    LOG.error("Failed to notify about event type {} unlock", unlocked, ex);
                }
            }
        }
        localLocking.updateLockedEventTypes(newList);
    }

    public Closeable workWithEventType(final String eventType, final long timeoutMs)
            throws TimeoutException, InterruptedException {
        return localLocking.workWithEventType(eventType, timeoutMs);
    }

    public TimelineSync.ListenerRegistration registerTimelineChangeListener(
            final String eventType,
            final Consumer<String> listener) {
        synchronized (consumerListeners) {
            if (!consumerListeners.containsKey(eventType)) {
                consumerListeners.put(eventType, new ArrayList<>());
            }
            consumerListeners.get(eventType).add(listener);
        }
        return () -> {
            synchronized (consumerListeners) {
                consumerListeners.get(eventType).remove(listener);
                if (consumerListeners.get(eventType).isEmpty()) {
                    consumerListeners.remove(eventType);
                }
            }
        };
    }

    public TimelineSync.ListenerRegistration registerTimelineChangeListener(final Consumer<String> listener) {
        synchronized (headlessConsumerListeners) {
            headlessConsumerListeners.add(listener);
        }
        return () -> {
            synchronized (headlessConsumerListeners) {
                headlessConsumerListeners.remove(listener);
            }
        };
    }

}
