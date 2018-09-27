package org.zalando.nakadi.repository.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.EventValidation;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EventTypeCache {

    public static final String ZKNODE_PATH = "/nakadi/event_types";
    public static final int CACHE_MAX_SIZE = 100000;
    private static final Logger LOG = LoggerFactory.getLogger(EventTypeCache.class);
    private final LoadingCache<String, CachedValue> eventTypeCache;
    private final PathChildrenCache cacheSync;
    private final ZooKeeperHolder zkClient;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final TimelineSync timelineSync;
    private final List<Consumer<String>> invalidationListeners = new CopyOnWriteArrayList<>();
    private Map<String, TimelineSync.ListenerRegistration> timelineRegistrations;

    public EventTypeCache(final EventTypeRepository eventTypeRepository,
                          final TimelineDbRepository timelineRepository,
                          final ZooKeeperHolder zkClient,
                          final TimelineSync timelineSync)
            throws Exception {
        this(eventTypeRepository, timelineRepository, zkClient, setupCacheSync(zkClient.get()), timelineSync);
    }

    @VisibleForTesting
    EventTypeCache(final EventTypeRepository eventTypeRepository,
                   final TimelineDbRepository timelineRepository,
                   final ZooKeeperHolder zkClient,
                   final PathChildrenCache cache,
                   final TimelineSync timelineSync) {
        this.zkClient = zkClient;
        this.eventTypeCache = setupInMemoryEventTypeCache(eventTypeRepository, timelineRepository);
        this.cacheSync = cache;
        this.timelineSync = timelineSync;
        this.timelineRegistrations = new ConcurrentHashMap<>();
        if (null != cacheSync) {
            this.cacheSync.getListenable().addListener((curator, event) -> this.onZkEvent(event));
        }
        preloadEventTypes(eventTypeRepository, timelineRepository);
    }

    private static PathChildrenCache setupCacheSync(final CuratorFramework zkClient) throws Exception {
        try {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ZKNODE_PATH);
        } catch (final KeeperException.NodeExistsException expected) {
            // silently do nothing since it means that the node is already there
        }

        final PathChildrenCache cacheSync = new PathChildrenCache(zkClient, ZKNODE_PATH, false);

        // It is important to preload all data before specifying callback for updates, because otherwise preload won't
        // give any effect - all changes will be removed.
        cacheSync.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        return cacheSync;
    }

    private void preloadEventTypes(final EventTypeRepository eventTypeRepository,
                                   final TimelineDbRepository timelineRepository) {
        final long start = System.currentTimeMillis();
        rwLock.writeLock().lock();
        try {
            final Map<String, List<Timeline>> eventTypeTimelines = timelineRepository.listTimelinesOrdered().stream()
                    .collect(Collectors.groupingBy(Timeline::getEventType));
            final List<Timeline> emptyList = ImmutableList.of();
            final Map<String, CachedValue> preloaded = eventTypeRepository.list().stream().collect(Collectors.toMap(
                    EventType::getName,
                    et -> new CachedValue(
                            et,
                            EventValidation.forType(et),
                            eventTypeTimelines.getOrDefault(et.getName(), emptyList))
            ));
            final Iterator<Map.Entry<String, CachedValue>> it = preloaded.entrySet().iterator();
            while (it.hasNext()) {
                String eventTypeName = null;
                try {
                    eventTypeName = it.next().getKey();
                    created(eventTypeName);
                } catch (final Exception e) {
                    LOG.error("Failed to create node for {}", eventTypeName, e);
                    it.remove();
                }
            }
            this.eventTypeCache.putAll(preloaded);
            LOG.info("Cache preload complete, load {} event types within {} ms",
                    preloaded.size(),
                    System.currentTimeMillis() - start);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void updated(final String name) throws Exception {
        created(name); // make sure every event type is tracked in the remote cache
        final String path = getZNodePath(name);
        zkClient.get().setData().forPath(path, new byte[0]);
    }

    public void created(final String name) throws Exception {
        try {
            final String path = getZNodePath(name);
            zkClient.get()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, new byte[0]);
        } catch (final KeeperException.NodeExistsException expected) {
            LOG.debug("Silently do nothing since event type has already been tracked");
        }

        timelineRegistrations.computeIfAbsent(name,
                n -> timelineSync.registerTimelineChangeListener(n, (etName) -> eventTypeCache.invalidate(etName)));
    }

    public void removed(final String name) throws Exception {
        final String path = getZNodePath(name);
        created(name); // make sure every nome is tracked in the remote cache
        zkClient.get().delete().forPath(path);
        timelineRegistrations.remove(name).cancel();
    }

    private Optional<CachedValue> getCached(final String name)
            throws NoSuchEventTypeException, InternalNakadiException {
        try {
            return Optional.ofNullable(eventTypeCache.get(name));
        } catch (final UncheckedExecutionException | ExecutionException e) {
            if (e.getCause() instanceof NoSuchEventTypeException) {
                throw (NoSuchEventTypeException) e.getCause();
            } else {
                throw new InternalNakadiException("Problem loading event type", e);
            }
        }
    }

    public EventType getEventType(final String name) throws NoSuchEventTypeException, InternalNakadiException {
        return getCached(name).map(CachedValue::getEventType)
                .orElseThrow(() -> new NoSuchEventTypeException("Event type " + name + " does not exists"));
    }

    public EventTypeValidator getValidator(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        return getCached(name).map(CachedValue::getEventTypeValidator)
                .orElseThrow(() -> new NoSuchEventTypeException("Event type " + name + " does not exists"));
    }

    public List<Timeline> getTimelinesOrdered(final String name) throws InternalNakadiException,
            NoSuchEventTypeException {
        return getCached(name)
                .orElseThrow(() -> new NoSuchEventTypeException("Event type " + name + " does not exists"))
                .getTimelines();
    }

    private void onZkEvent(final PathChildrenCacheEvent event) {
        // Lock is needed only to support massive load on startup. In all other cases it will be called for
        // event type creation/update, so it won't create any additional load.
        String invalidatedEventType = null;
        rwLock.readLock().lock();
        try {
            final boolean needInvalidate = event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED ||
                    event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED ||
                    event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED;
            if (needInvalidate) {
                final String[] path = event.getData().getPath().split("/");
                invalidatedEventType = path[path.length - 1];
                eventTypeCache.invalidate(invalidatedEventType);
            }
        } finally {
            rwLock.readLock().unlock();
        }
        if (null != invalidatedEventType) {
            for (final Consumer<String> listener : invalidationListeners) {
                listener.accept(invalidatedEventType);
            }
        }
    }

    private LoadingCache<String, CachedValue> setupInMemoryEventTypeCache(
            final EventTypeRepository eventTypeRepository, final TimelineDbRepository timelineRepository) {
        final CacheLoader<String, CachedValue> loader = new CacheLoader<String, CachedValue>() {
            public CachedValue load(final String key) throws Exception {
                final EventType eventType = eventTypeRepository.findByName(key);
                final List<Timeline> timelines = timelineRepository.listTimelinesOrdered(key);
                timelineRegistrations.computeIfAbsent(key, n ->
                        timelineSync.registerTimelineChangeListener(n, (etName) -> eventTypeCache.invalidate(etName)));
                return new CachedValue(eventType, EventValidation.forType(eventType), timelines);
            }
        };

        return CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE).build(loader);
    }

    private String getZNodePath(final String eventTypeName) {
        return ZKPaths.makePath(ZKNODE_PATH, eventTypeName);
    }

    public void addInvalidationListener(final Consumer<String> onEventTypeInvalidated) {
        invalidationListeners.add(onEventTypeInvalidated);
    }

    private static class CachedValue {
        private final EventType eventType;
        private final EventTypeValidator eventTypeValidator;
        @Nonnull
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
