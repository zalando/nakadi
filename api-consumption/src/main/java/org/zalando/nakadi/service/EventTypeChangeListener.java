package org.zalando.nakadi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.cache.EventTypeCache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

@Component
public class EventTypeChangeListener {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Map<String, Set<InternalListener>> eventTypeToListeners = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(EventTypeChangeListener.class);

    private class InternalListener implements Closeable {
        private boolean closed = false;
        private final Collection<String> eventTypes;
        private final Consumer<String> authorizationChangeListener;

        private InternalListener(
                final Collection<String> eventTypes, final Consumer<String> authorizationChangeListener) {
            this.eventTypes = eventTypes;
            this.authorizationChangeListener = authorizationChangeListener;
        }

        public Collection<String> getEventTypes() {
            return eventTypes;
        }

        @Override
        public void close() {
            if (closed) {
                LOG.warn("Second attempt to close event type authorization listener " + authorizationChangeListener);
                return;
            }
            closed = true;
            unregisterListener(this);
        }
    }

    @Autowired
    public EventTypeChangeListener(final EventTypeCache eventTypeCache) {
        eventTypeCache.addInvalidationListener(this::onEventTypeInvalidated);
    }

    private void onEventTypeInvalidated(final String eventType) {
        final Optional<Set<InternalListener>> toNotify;
        readWriteLock.readLock().lock();
        try {
            toNotify = Optional.ofNullable(eventTypeToListeners.get(eventType))
                    .map(HashSet<InternalListener>::new);
        } finally {
            readWriteLock.readLock().unlock();
        }
        if (toNotify.isPresent()) {
            for (final InternalListener listener : toNotify.get()) {
                try {
                    listener.authorizationChangeListener.accept(eventType);
                } catch (RuntimeException ex) {
                    LOG.error("Failed to notify listener " + listener, ex);
                }
            }
        }
    }

    // It is impossible to unregister listener while receiving notification via authorizationChangeListener
    public Closeable registerListener(
            final Consumer<String> authorizationChangeListener,
            final Collection<String> eventTypes) {
        final InternalListener listener = new InternalListener(eventTypes, authorizationChangeListener);
        readWriteLock.writeLock().lock();
        try {
            eventTypes.stream()
                    .map(eventType -> eventTypeToListeners.computeIfAbsent(
                            eventType,
                            e -> new HashSet<InternalListener>()))
                    .forEach(cachedData -> cachedData.add(listener));
        } finally {
            readWriteLock.writeLock().unlock();
        }
        return listener;
    }

    private void unregisterListener(final InternalListener listener) {
        readWriteLock.writeLock().lock();
        try {
            for (final String eventType : listener.getEventTypes()) {
                final Set<InternalListener> cachedData = eventTypeToListeners.get(eventType);
                cachedData.remove(listener);
                if (cachedData.isEmpty()) {
                    eventTypeToListeners.remove(eventType);
                }
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
