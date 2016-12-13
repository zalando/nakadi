package org.zalando.nakadi.repository.db;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.EventValidation;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.concurrent.ExecutionException;

public class EventTypeCache {

    public static final String ZKNODE_PATH = "/nakadi/event_types";
    public static final int CACHE_MAX_SIZE = 100000;
    private static final Logger LOG = LoggerFactory.getLogger(EventTypeCache.class);

    private final LoadingCache<String, EventType> eventTypeCache;
    private final LoadingCache<String, EventTypeValidator> validatorCache;
    private final PathChildrenCache cacheSync;
    private final CuratorFramework zkClient;

    public EventTypeCache(final EventTypeRepository eventTypeRepository, final CuratorFramework zkClient)
            throws Exception {
        initParentCacheZNode(zkClient);

        this.zkClient = zkClient;
        this.eventTypeCache = setupInMemoryEventTypeCache(eventTypeRepository);
        this.validatorCache = setupInMemoryValidatorCache(eventTypeCache);
        this.cacheSync = setupCacheSync(zkClient);
        preloadEventTypes(eventTypeRepository);
    }

    private void preloadEventTypes(final EventTypeRepository eventTypeRepository) {
        final long start = System.currentTimeMillis();
        eventTypeRepository.list().stream().map(EventType::getName).forEach((name) -> {
            try {
                this.getEventType(name);
            } catch (NoSuchEventTypeException | InternalNakadiException e) {
                LOG.warn("Failed to preload event type {}", name, e);
            }
        });
        LOG.info("Cache preload complete, load {} event types within {} ms",
                this.eventTypeCache.size(),
                System.currentTimeMillis() - start);
    }

    public void updated(final String name) throws Exception {
        final String path = getZNodePath(name);
        created(name); // make sure every event type is tracked in the remote cache
        zkClient.setData().forPath(path, new byte[0]);
    }

    public EventType getEventType(final String name) throws NoSuchEventTypeException, InternalNakadiException {
        try {
            return eventTypeCache.get(name);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchEventTypeException) {
                final NoSuchEventTypeException noSuchEventTypeException = (NoSuchEventTypeException) e.getCause();
                throw noSuchEventTypeException;
            } else {
                throw new InternalNakadiException("Problem loading event type", e);
            }
        }
    }

    public EventTypeValidator getValidator(final String name) throws ExecutionException {
        return validatorCache.get(name);
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

    private void initParentCacheZNode(final CuratorFramework zkClient) throws Exception {
        try {
            zkClient
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ZKNODE_PATH);
        } catch (KeeperException.NodeExistsException expected) {
            // silently do nothing since it means that the node is already there
        }
    }

    private void addCacheChangeListener(final LoadingCache<String, EventType> eventTypeCache,
                                        final LoadingCache<String, EventTypeValidator> validatorCache,
                                        final PathChildrenCache cacheSync) {
        final PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event) throws Exception {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    invalidateCacheKey(event);
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    invalidateCacheKey(event);
                }
            }

            private void invalidateCacheKey(final PathChildrenCacheEvent event) {
                final String path[] = event.getData().getPath().split("/");
                final String key = path[path.length - 1];

                validatorCache.invalidate(key);
                eventTypeCache.invalidate(key);
            }
        };

        cacheSync.getListenable().addListener(listener);
    }

    private PathChildrenCache setupCacheSync(final CuratorFramework zkClient) throws Exception {
        final PathChildrenCache cacheSync = new PathChildrenCache(zkClient, ZKNODE_PATH, false);

        cacheSync.start();

        addCacheChangeListener(eventTypeCache, validatorCache, cacheSync);

        return cacheSync;
    }

    private LoadingCache<String, EventTypeValidator> setupInMemoryValidatorCache(
            final LoadingCache<String, EventType> eventTypeCache) {
        final CacheLoader<String, EventTypeValidator> loader = new CacheLoader<String, EventTypeValidator>() {
            public EventTypeValidator load(final String key) throws Exception {
                final EventType et = eventTypeCache.get(key);
                return EventValidation.forType(et);
            }
        };

        return CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE).build(loader);
    }

    private LoadingCache<String, EventType> setupInMemoryEventTypeCache(final EventTypeRepository eventTypeRepository) {
        final CacheLoader<String, EventType> loader = new CacheLoader<String, EventType>() {
            public EventType load(final String key) throws Exception {
                final EventType eventType = eventTypeRepository.findByName(key);
                created(key); // make sure that all event types are tracked in the remote cache
                return eventType;
            }
        };

        return CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE).build(loader);
    }

    private String getZNodePath(final String eventTypeName) {
        return ZKPaths.makePath(ZKNODE_PATH, eventTypeName);
    }
}
