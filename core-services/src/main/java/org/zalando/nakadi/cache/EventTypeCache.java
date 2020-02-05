package org.zalando.nakadi.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.db.EventTypeDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.EventValidatorBuilder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Service
public class EventTypeCache {
    private final ChangeSet currentChangeSet = new ChangeSet();
    private final ChangesRegistry changesRegistry;
    private final LoadingCache<String, CachedValue> valueCache;
    private final EventTypeDbRepository eventTypeDbRepository;
    private final TimelineDbRepository timelineRepository;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicLong lastCheck = new AtomicLong();
    private final AtomicLong watcherCounter = new AtomicLong();
    private final List<Consumer<String>> invalidationListeners = new ArrayList<>();
    private final TimelineSync timelineSync;
    private final long periodicUpdatesInterval;
    private final long zkChangesTTL;
    private final EventValidatorBuilder eventValidatorBuilder;
    private TimelineSync.ListenerRegistration timelineSyncListener = null;

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeCache.class);

    @Autowired
    public EventTypeCache(
            final ChangesRegistry changesRegistry,
            final EventTypeDbRepository eventTypeDbRepository,
            final TimelineDbRepository timelineRepository,
            final TimelineSync timelineSync,
            final EventValidatorBuilder eventValidatorBuilder,
            @Value("${nakadi.event-cache.periodic-update-seconds:120}") final long periodicUpdatesIntervalSeconds,
            @Value("${nakadi.event-cache.change-ttl:600}") final long zkChangesTTLSeconds) {
        this.changesRegistry = changesRegistry;
        this.eventTypeDbRepository = eventTypeDbRepository;
        this.timelineRepository = timelineRepository;
        this.valueCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofHours(2))
                .build(CacheLoader.from(this::loadValue));
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.timelineSync = timelineSync;
        this.eventValidatorBuilder = eventValidatorBuilder;
        this.periodicUpdatesInterval = TimeUnit.SECONDS.toMillis(periodicUpdatesIntervalSeconds);
        this.zkChangesTTL = TimeUnit.SECONDS.toMillis(zkChangesTTLSeconds);
    }

    @PostConstruct
    public void startUpdates() {
        this.timelineSyncListener = timelineSync.registerTimelineChangeListener(this::invalidateFromRemote);
        watcherCounter.set(0L);
        // Schedule periodic updates, so that first update will recreate zookeeper notification
        lastCheck.set(System.currentTimeMillis());
        // Register listener
        scheduledExecutorService.submit(this::reprocessUntilExceptionDisappears);
        // Periodic checks
        scheduledExecutorService.submit(this::periodicCheck);
        LOG.info("Started updates");
    }

    @PreDestroy
    public void stopUpdates() {
        LOG.info("Stopping updates");
        try {
            timelineSyncListener.cancel();
        } finally {
            timelineSyncListener = null;
            scheduledExecutorService.shutdown();
        }
    }

    private void getUpdatesAndRegisterListener() throws Exception {
        // Register watcher, only last one will be used anyways.
        final long nextWatcherVersion = this.watcherCounter.incrementAndGet();
        final List<Change> changes = changesRegistry.getCurrentChanges(
                () -> reactOnZookeeperChangesExternal(nextWatcherVersion));

        final Collection<String> updatedEventTypes = this.currentChangeSet.getUpdatedEventTypes(changes);
        updatedEventTypes.forEach(this::invalidateFromRemote);

        final List<String> changeIdsToRemove = currentChangeSet.getChangesToRemove(changes, zkChangesTTL).stream()
                .map(Change::getId)
                .collect(Collectors.toList());
        if (!changeIdsToRemove.isEmpty()) {
            LOG.info("Detected changes to remove, will try to remove {}", String.join(", ", changeIdsToRemove));
            this.changesRegistry.deleteChanges(changeIdsToRemove);
        }
        this.lastCheck.set(System.currentTimeMillis());
    }

    private void reactOnZookeeperChangesLocal(final long watcherVersion) {
        if (watcherVersion != watcherCounter.get()) {
            LOG.warn("Watcher notification is ignored, as probably there are several watchers");
        } else {
            reprocessUntilExceptionDisappears();
        }
    }

    private void reprocessUntilExceptionDisappears() {
        try {
            getUpdatesAndRegisterListener();
        } catch (Exception ex) {
            LOG.warn("Failed to register listener and process updates", ex);
            this.scheduledExecutorService.schedule(this::reprocessUntilExceptionDisappears, 5, TimeUnit.SECONDS);
        }
    }

    private void reactOnZookeeperChangesExternal(final long watcherVersion) {
        // Triggered on change from zk, and is executed on zk thread, therefore should be registered scheduled executor.
        scheduledExecutorService.submit(() -> reactOnZookeeperChangesLocal(watcherVersion));
    }

    private void periodicCheck() {
        final long deltaMillis = (System.currentTimeMillis() - this.lastCheck.get());
        // every 2 minutes
        // Ensure that we are not overreacting...
        if (deltaMillis < periodicUpdatesInterval) {
            this.scheduledExecutorService.schedule(
                    this::periodicCheck, periodicUpdatesInterval - deltaMillis, TimeUnit.MILLISECONDS);
            return;
        }
        final boolean haveChanges;
        try {
            final List<Change> currentChanges = changesRegistry.getCurrentChanges(null);
            haveChanges = !currentChangeSet.getUpdatedEventTypes(currentChanges).isEmpty()
                    || !currentChangeSet.getChangesToRemove(currentChanges, zkChangesTTL).isEmpty();
        } catch (final Exception e) {
            LOG.warn("Failed to run periodic check, will retry soon", e);
            scheduledExecutorService.schedule(this::periodicCheck, 1, TimeUnit.SECONDS);
            return;
        }
        if (haveChanges) {
            reprocessUntilExceptionDisappears();
        }
        scheduledExecutorService.schedule(this::periodicCheck, periodicUpdatesInterval, TimeUnit.MILLISECONDS);
    }

    // Received notification that value was invalidated externally
    private void invalidateFromRemote(final String eventType) {
        LOG.info("Invalidating event type {} because of remote notification", eventType);
        invalidateInternal(eventType);
    }

    private void invalidateInternal(final String eventType) {
        try {
            this.valueCache.invalidate(eventType);
            invalidationListeners.forEach(l -> l.accept(eventType));
        } catch (RuntimeException ex) {
            LOG.error("Failed to react on external value invalidation. Wait for next update", ex);
        }
    }

    // Local code asked to invalidate value
    public void invalidate(final String eventTypeName) {
        LOG.info("Invalidating event type {} and triggering changes notification", eventTypeName);
        invalidateInternal(eventTypeName);
        try {
            this.changesRegistry.registerChange(eventTypeName);
        } catch (final Exception ex) {
            LOG.error("Failed to register invalidation requests for event type {}. " +
                            "If it is required - update manually again",
                    eventTypeName,
                    ex);
        }
    }

    public EventType getEventType(final String name) throws NoSuchEventTypeException {
        return getCached(name).getEventType();
    }

    public EventTypeValidator getValidator(final String name) throws NoSuchEventTypeException {
        return getCached(name).getEventTypeValidator();
    }

    public List<Timeline> getTimelinesOrdered(final String name) throws NoSuchEventTypeException {
        return getCached(name).getTimelines();
    }

    private CachedValue getCached(final String name) throws NoSuchEventTypeException {
        try {
            return this.valueCache.getUnchecked(name);
        } catch (UncheckedExecutionException ex) {
            if (ex.getCause() instanceof NoSuchEventTypeException) {
                throw (NoSuchEventTypeException) ex.getCause();
            } else {
                throw new InternalNakadiException("Failed to get event type", ex);
            }
        }
    }

    private CachedValue loadValue(final String eventTypeName) {
        final long start = System.currentTimeMillis();
        final EventType eventType = eventTypeDbRepository.findByName(eventTypeName);

        final List<Timeline> timelines =
                timelineRepository.listTimelinesOrdered(eventTypeName);

        final CachedValue result = new CachedValue(
                eventType,
                eventValidatorBuilder.build(eventType),
                timelines
        );
        LOG.info("Successfully load event type {}, took: {} ms", eventTypeName, System.currentTimeMillis() - start);
        return result;
    }

    public void addInvalidationListener(final Consumer<String> listener) {
        synchronized (this.invalidationListeners) {
            this.invalidationListeners.add(listener);
        }
    }

    private static class CachedValue {
        private final EventType eventType;
        private final EventTypeValidator eventTypeValidator;
        private final List<Timeline> timelines;

        CachedValue(final EventType eventType,
                    final EventTypeValidator eventTypeValidator,
                    final List<Timeline> timelines) {
            this.eventType = eventType;
            this.eventTypeValidator = eventTypeValidator;
            this.timelines = timelines;
        }

        public EventType getEventType() {
            return eventType;
        }

        public EventTypeValidator getEventTypeValidator() {
            return eventTypeValidator;
        }

        public List<Timeline> getTimelines() {
            return timelines;
        }
    }

}
