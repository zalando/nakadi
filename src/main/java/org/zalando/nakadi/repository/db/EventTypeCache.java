package org.zalando.nakadi.repository.db;

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
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.EventValidation;

import java.util.ArrayList;
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

    private static class CachedValue {
        final EventType eventType;
        final EventTypeValidator eventTypeValidator;

        public CachedValue(final EventType eventType, final EventTypeValidator eventTypeValidator) {
            this.eventType = eventType;
            this.eventTypeValidator = eventTypeValidator;
        }

        public EventType getEventType() {
            return eventType;
        }

        public EventTypeValidator getEventTypeValidator() {
            return eventTypeValidator;
        }
    }

    private final LoadingCache<String, CachedValue> eventTypeCache;
    private final PathChildrenCache cacheSync;
    private final CuratorFramework zkClient;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public EventTypeCache(final EventTypeRepository eventTypeRepository, final CuratorFramework zkClient)
            throws Exception {
        this.zkClient = zkClient;
        this.eventTypeCache = setupInMemoryEventTypeCache(eventTypeRepository);
        this.cacheSync = setupCacheSync(zkClient);
        preloadEventTypes(eventTypeRepository);
    }

    /**
     * Preloads cache.
     * The problem with preload is that notifications about modifications may be skipped while initializing cache.
     * That is why we are using lock for updates. In normal case (without preload) this is covered by LoadingCache
     * by itself
     *
     * @param eventTypeRepository
     */
    private void preloadEventTypes(final EventTypeRepository eventTypeRepository) {
        final long start = System.currentTimeMillis();
        rwLock.writeLock().lock();
        try {
            final Map<String, CachedValue> preloaded = eventTypeRepository.list().stream().collect(Collectors.toMap(
                    EventType::getName,
                    et -> new CachedValue(et, EventValidation.forType(et))
            ));
            new ArrayList<>(preloaded.keySet()).forEach(eventType -> {
                try {
                    created(eventType);
                } catch (Exception e) {
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
        zkClient.setData().forPath(path, new byte[0]);
    }

    public void created(final String name) throws Exception {
        try {
            final String path = getZNodePath(name);
            zkClient
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, new byte[0]);
        } catch (KeeperException.NodeExistsException expected) {
            // silently do nothing since it's already been tracked
        }
    }

    public void removed(final String name) throws Exception {
        final String path = getZNodePath(name);
        created(name); // make sure every nome is tracked in the remote cache
        zkClient.delete().forPath(path);
    }

    private Optional<CachedValue> getCached(final String name)
            throws NoSuchEventTypeException, InternalNakadiException {
        try {
            return Optional.ofNullable(eventTypeCache.get(name));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchEventTypeException) {
                throw (NoSuchEventTypeException) e.getCause();
            } else {
                throw new InternalNakadiException("Problem loading event type", e);
            }
        }
    }

    public EventType getEventType(final String name) throws NoSuchEventTypeException, InternalNakadiException {
        return getCached(name).map(CachedValue::getEventType).orElse(null);
    }

    public EventTypeValidator getValidator(final String name) throws InternalNakadiException, NoSuchEventTypeException {
        return getCached(name).map(CachedValue::getEventTypeValidator).orElse(null);
    }

    private PathChildrenCache setupCacheSync(final CuratorFramework zkClient) throws Exception {
        try {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ZKNODE_PATH);
        } catch (KeeperException.NodeExistsException expected) {
            // silently do nothing since it means that the node is already there
        }

        final PathChildrenCache cacheSync = new PathChildrenCache(zkClient, ZKNODE_PATH, false);

        cacheSync.start();

        cacheSync.getListenable().addListener((curator, event) -> this.onZkEvent(event));

        return cacheSync;
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
            final EventTypeRepository eventTypeRepository) {
        final CacheLoader<String, CachedValue> loader = new CacheLoader<String, CachedValue>() {
            public CachedValue load(final String key) throws Exception {
                final EventType eventType = eventTypeRepository.findByName(key);
                return new CachedValue(eventType, EventValidation.forType(eventType));
            }
        };

        return CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE).build(loader);
    }

    private String getZNodePath(final String eventTypeName) {
        return ZKPaths.makePath(ZKNODE_PATH, eventTypeName);
    }
}
