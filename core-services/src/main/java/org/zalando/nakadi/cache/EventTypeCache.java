package org.zalando.nakadi.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.EventValidatorBuilder;
import org.zalando.nakadi.validation.JsonSchemaValidator;
import org.zalando.nakadi.validation.ValidationError;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * <b>
 * Distributed event type cache, based on zookeeper notifications about changes.</b>
 * <p>
 * On every update of event type cache, new node is created in zookeeper, and all the instances, that are listening
 * for children changes on a parent zookeeper node are reading contents of all the active changes, and apply them.
 * As it is known, that sometimes zookeeper/curator under unknown circumstances are loosing watchers, and we have
 * to think about recreation of the watcher if it is needed.
 * </p>
 * <p>
 * How it works on particular instance from 2 different points of view:
 *     <ol>
 *         <li>Event type has changed/deleted on this instance and instance tries to notify others about the change.
 *         <ul>
 *             <li>local cache for event type is invalidated and notifications about it are issued to all local
 *             consumers</li>
 *             <li>new change with event type name is stored in zookeeper</li>
 *         </ul>
 *         </li>
 *         <li>There is notification about new change issued by zookeeper.
 *         <ul>
 *             <li>Notification is triggered from zk</li>
 *             <li>Full changeset is read from zookeeper and compared with local version of changeset. In case if there
 *             are changes (event was updated/deleted somehow), local cache for changed event types is invalidated and
 *             notifications are issued to local consumers</li>
 *             <li>Watcher for change notifications is recreated</li>
 *         </ul>
 *         </li>
 *     </ol>
 * </p>
 * <p>
 * However, there are 2 corner cases with this approach:
 * <ol>
 *     <li> Zookeeper for some reasons may forget to notify about change.
 *     In order to tackle this problem, cache is periodically (with {@code periodicUpdatesIntervalSeconds} interval)
 * trying to get current list of changes from zookeeper. In case if there are modifications to the list of changes -
 * listener in zookeeper is recreated and notifications are processed. This way we guarantee, that all the changes will
 * be propagated if they were written to zk.
 *     </li>
 *     <li>Some update was written to database, but for some reason change was not propagated to zookeeper. There are 2
 * possible reasons for that:
 *     <ul>
 *          <li>zookeeper was unavailable at the moment</li>
 *          <li>instance was terminated for some reason right after updating database.</li>
 *     </ul>
 * In order to tackle this problem there is TTL for value in cache, which is relatively high (2 hours hardcoded).
 * The reason is that client anyways will receive 5XX response on event type update and will retry, therefore cache
 * anyways will be reset from other instance. If it will not work out many times in a row (or incase of fire and forget
 * requests), then in will be reset by this huge TTL.
 *     </li>
 * </ol>
 * </p>
 *
 * <p>
 * And the last part - in order to continue operating fast enough, we have to have relatively short list of changes
 * stored in zookeeper. This is regulated by {@code zkChangesTTLSeconds}. All the changes that are living longer than
 * this period of time are removed from zookeeper.
 * Because of distributed nature of nakadi, one can not rely on time that is written in zookeeper or time that is
 * generated by nakadi. Therefore, instance is using it's own time all the way while working with changeset. Thus,
 * we are getting several benefits: <ol>
 *     <li>Probably only one instance will be trying to remove the changes.</li>
 *     <li>Instances may run with slightly different times on it and cache parameters could be changes to something
 *     really small, like several seconds.</li>
 * </ol>
 * </p>
 *
 * <p>Considerations in regards to cache eviction after write are:
 * Suppose that we have 1000 event types that are heavily used, and are running 100 instances. Then, within eviction
 * interval there will be at least 1000 * 100 = 100_000 cache loads for event type from the cache.
 * In case if eviction interval is 1 minute, that means we will have ~1667 guaranteed cache loads per second.
 * In case if eviction interval is 2 hours, than there will be ~ 14 guaranteed cache loads per second.
 * </p>
 */
@Service
public class EventTypeCache {
    private final ChangeSet currentChangeSet = new ChangeSet();
    private final ChangesRegistry changesRegistry;
    private final LoadingCache<String, CachedValue> valueCache;
    private final EventTypeRepository eventTypeRepository;
    private final TimelineDbRepository timelineRepository;
    private final TopicRepositoryHolder topicRepositoryHolder;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicLong lastCheck = new AtomicLong();
    private final AtomicLong watcherCounter = new AtomicLong();
    private final List<Consumer<String>> invalidationListeners = new ArrayList<>();
    private final TimelineSync timelineSync;
    private final long periodicUpdatesInterval;
    private final long zkChangesTTL;
    private final EventValidatorBuilder eventValidatorBuilder;
    private final SchemaService schemaService;
    private TimelineSync.ListenerRegistration timelineSyncListener = null;

    private static final Logger LOG = LoggerFactory.getLogger(EventTypeCache.class);

    @Autowired
    public EventTypeCache(
            final ChangesRegistry changesRegistry,
            final EventTypeRepository eventTypeRepository,
            final TimelineDbRepository timelineRepository,
            final TopicRepositoryHolder topicRepositoryHolder,
            final TimelineSync timelineSync,
            final EventValidatorBuilder eventValidatorBuilder,
            @Lazy final SchemaService schemaService,
            @Value("${nakadi.event-cache.periodic-update-seconds:120}") final long periodicUpdatesIntervalSeconds,
            @Value("${nakadi.event-cache.change-ttl:600}") final long zkChangesTTLSeconds) {
        this.changesRegistry = changesRegistry;
        this.eventTypeRepository = eventTypeRepository;
        this.timelineRepository = timelineRepository;
        this.topicRepositoryHolder = topicRepositoryHolder;
        this.valueCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofHours(2))
                .build(CacheLoader.from(this::loadValue));
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.timelineSync = timelineSync;
        this.eventValidatorBuilder = eventValidatorBuilder;
        this.schemaService = schemaService;
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
        this.currentChangeSet.apply(changes);
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
            // avoid logging the stacktrace: it happens often, but it's not that interesting
            LOG.warn("Failed to register listener and process updates: {}", ex.toString());
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
            LOG.warn("Failed to run periodic check, will retry soon", e.toString());
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

    public JsonSchemaValidator getValidator(final String name) throws NoSuchEventTypeException {
        return getCached(name).getJsonSchemaValidator();
    }

    public List<Timeline> getTimelinesOrdered(final String name) throws NoSuchEventTypeException {
        return getCached(name).getTimelinesOrdered();
    }

    public List<String> getOrderedPartitions(final String name) throws NoSuchEventTypeException {
        return getCached(name).getOrderedPartitions();
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
        final EventType eventType = eventTypeRepository.findByName(eventTypeName);

        final JsonSchemaValidator validator;
        //
        // The validator is used for publishing JSON events, but the event type may be already
        // converted to Avro, so try to find latest JSON schema, if any:
        //
        if (eventType.getSchema().getType() != EventTypeSchema.Type.JSON_SCHEMA) {
            final Optional<EventTypeSchema> schema = schemaService.getLatestSchemaByType(eventTypeName,
                    EventTypeSchema.Type.JSON_SCHEMA);

            if (schema.isPresent()) {
                eventType.setLatestSchemaByType(schema.get());
                validator = eventValidatorBuilder.build(eventType);
            } else {
                validator = new NoJsonSchemaValidator(eventTypeName);
            }
        } else {
            validator = eventValidatorBuilder.build(eventType);
        }

        final List<Timeline> timelines =
                timelineRepository.listTimelinesOrdered(eventTypeName);
        //
        // 1. Historically ordering as strings, even though partition "names" are numeric.
        // 2. Using ArrayList to get fast access by index for actual partitioning.
        //
        final List<String> orderedPartitions =
                TimelineService.getActiveTimeline(timelines)
                .map(timeline ->
                        topicRepositoryHolder.getTopicRepository(timeline.getStorage())
                        .listPartitionNames(timeline.getTopic())
                        .stream()
                        .sorted()
                        .collect(Collectors.toCollection(ArrayList::new)))
                .orElseThrow(() -> {
                            throw new InternalNakadiException("No active timeline found for event type: "
                                    + eventTypeName);
                        });

        LOG.info("Successfully load event type {}, took: {} ms", eventTypeName, System.currentTimeMillis() - start);

        return new CachedValue(eventType, validator, timelines, Collections.unmodifiableList(orderedPartitions));
    }

    private class NoJsonSchemaValidator implements JsonSchemaValidator {
        final String message;

        private NoJsonSchemaValidator(final String eventTypeName) {
            this.message = "No json_schema found for event type: " + eventTypeName;
        }

        @Override
        public Optional<ValidationError> validate(final JSONObject event) {
            return Optional.of(new ValidationError(message));
        }
    }

    public void addInvalidationListener(final Consumer<String> listener) {
        synchronized (this.invalidationListeners) {
            this.invalidationListeners.add(listener);
        }
    }

    private static class CachedValue {
        private final EventType eventType;
        private final JsonSchemaValidator jsonSchemaValidator;
        private final List<Timeline> timelinesOrdered;
        private final List<String> orderedPartitions;

        CachedValue(final EventType eventType,
                    final JsonSchemaValidator jsonSchemaValidator,
                    final List<Timeline> timelinesOrdered,
                    final List<String> orderedPartitions) {
            this.eventType = eventType;
            this.jsonSchemaValidator = jsonSchemaValidator;
            this.timelinesOrdered = timelinesOrdered;
            this.orderedPartitions = orderedPartitions;
        }

        public EventType getEventType() {
            return eventType;
        }

        public JsonSchemaValidator getJsonSchemaValidator() {
            return jsonSchemaValidator;
        }

        public List<Timeline> getTimelinesOrdered() {
            return timelinesOrdered;
        }

        public List<String> getOrderedPartitions() {
            return orderedPartitions;
        }
    }

    public Optional<EventType> getEventTypeIfExists(final String eventTypeName) throws InternalNakadiException {
        try {
            return Optional.of(getEventType(eventTypeName));
        } catch (final NoSuchEventTypeException e) {
            return Optional.empty();
        }
    }
}
