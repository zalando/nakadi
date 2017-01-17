package org.zalando.nakadi.repository.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.EventValidation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class EventTypeCache {

    public static final String ZKNODE_PATH = "/nakadi/event_types";
    public static final int CACHE_MAX_SIZE = 100000;
    private static final Logger LOG = LoggerFactory.getLogger(EventTypeCache.class);
    private final LoadingCache<String, CachedValue> eventTypeCache;
    private final PathChildrenCache cacheSync;
    private final ZooKeeperHolder zkClient;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    public EventTypeCache(final EventTypeRepository eventTypeRepository,
                          final TimelineDbRepository timelineRepository,
                          final ZooKeeperHolder zkClient)
            throws Exception {
        this(eventTypeRepository, timelineRepository, zkClient, setupCacheSync(zkClient.get()));
    }

    @VisibleForTesting
    EventTypeCache(final EventTypeRepository eventTypeRepository,
                   final TimelineDbRepository timelineRepository,
                   final ZooKeeperHolder zkClient,
                   final PathChildrenCache cache) {
        this.zkClient = zkClient;
        this.eventTypeCache = setupInMemoryEventTypeCache(eventTypeRepository, timelineRepository);
        this.cacheSync = cache;
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
            final Map<String, List<Timeline>> eventTypeTimelines = timelineRepository.list().stream()
                    .collect(Collectors.groupingBy(Timeline::getEventType));
            final Map<String, CachedValue> preloaded = eventTypeRepository.list().stream().collect(Collectors.toMap(
                    EventType::getName,
                    et -> new CachedValue(et, EventValidation.forType(et), eventTypeTimelines.get(et))
            ));
            new ArrayList<>(preloaded.keySet()).forEach(eventType -> {
                try {
                    created(eventType);
                } catch (final Exception e) {
                    LOG.error("Failed to create node for {}", eventType, e);
                    preloaded.remove(eventType);
                }
            });
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
            // silently do nothing since it's already been tracked
        }
    }

    public void removed(final String name) throws Exception {
        final String path = getZNodePath(name);
        created(name); // make sure every nome is tracked in the remote cache
        zkClient.get().delete().forPath(path);
    }

    private Optional<CachedValue> getCached(final String name)
            throws NoSuchEventTypeException, InternalNakadiException {
        try {
            return Optional.ofNullable(eventTypeCache.get(name));
        } catch (final ExecutionException e) {
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

    private void onZkEvent(final PathChildrenCacheEvent event) {
        // Lock is needed only to support massive load on startup. In all other cases it will be called for
        // event type creation/update, so it won't create any additional load.
        rwLock.readLock().lock();
        try {
            final boolean needInvalidate = event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED ||
                    event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED ||
                    event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED;
            if (needInvalidate) {
                final String[] path = event.getData().getPath().split("/");
                eventTypeCache.invalidate(path[path.length - 1]);
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private LoadingCache<String, CachedValue> setupInMemoryEventTypeCache(
            final EventTypeRepository eventTypeRepository, final TimelineDbRepository timelineRepository) {
        final CacheLoader<String, CachedValue> loader = new CacheLoader<String, CachedValue>() {
            public CachedValue load(final String key) throws Exception {
                final EventType eventType = eventTypeRepository.findByName(key);
                final List<Timeline> timelines = timelineRepository.listTimelines(key);
                return new CachedValue(eventType, EventValidation.forType(eventType), timelines);
            }
        };

        return CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE).build(loader);
    }

    private String getZNodePath(final String eventTypeName) {
        return ZKPaths.makePath(ZKNODE_PATH, eventTypeName);
    }

    private static class CachedValue {
        private final EventType eventType;
        private final EventTypeValidator eventTypeValidator;
        private final List<Timeline> timelines;

        public CachedValue(final EventType eventType,
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

        public Optional<Timeline> getActiveTimeline() {
            return timelines.stream()
                    .filter(t -> t.getSwitchedAt() != null)
                    .max(Comparator.comparing(Timeline::getOrder));
        }

        public List<Timeline> getTimelines() {
            return timelines;
        }
    }
}
