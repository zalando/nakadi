package de.zalando.aruha.nakadi.repository.db;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.validation.EventBodyMustRespectSchema;
import de.zalando.aruha.nakadi.validation.EventTypeValidator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.concurrent.ExecutionException;

public class EventTypeCache {

    public final String ZKNODE_PATH = "/nakadi/event_types";

    private final LoadingCache<String, EventType> eventTypeCache;
    private final LoadingCache<String, EventTypeValidator> validatorCache;
    private final PathChildrenCache cacheSync;
    private final CuratorFramework zkClient;

    public EventTypeCache(final EventTypeRepository dbRepo, final CuratorFramework zkClient) throws Exception {
        initParentCacheZNode(zkClient);

        this.zkClient = zkClient;
        this.eventTypeCache = setupInMemoryEventTypeCache(dbRepo);
        this.validatorCache = setupInMemoryValidatorCache(eventTypeCache);
        this.cacheSync = setupCacheSync(zkClient);
    }

    public void updated(final String name) throws Exception {
        final String path = getZNodePath(name);
        zkClient.setData().forPath(path, new byte[0]);
    }

    public EventType getEventType(final String name) throws NoSuchEventTypeException, InternalNakadiException {
        try {
            return eventTypeCache.get(name);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchEventTypeException) {
                NoSuchEventTypeException noSuchEventTypeException = (NoSuchEventTypeException) e.getCause();
                throw noSuchEventTypeException;
            } else {
                throw new InternalNakadiException("Problem loading event type", e);
            }
        }
    }

    public EventTypeValidator getValidator(final String name) throws ExecutionException {
        return validatorCache.get(name);
    }

    public void created(final EventType eventType) throws Exception {
        final String path = getZNodePath(eventType.getName());
        zkClient
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, new byte[0]);
    }

    public void removed(final String name) throws Exception {
        final String path = ZKPaths.makePath(ZKNODE_PATH, name);
        zkClient.delete().forPath(path);
    }

    private void initParentCacheZNode(final CuratorFramework zkClient) throws Exception {
        try {
            zkClient
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ZKNODE_PATH);
        } catch (KeeperException.NodeExistsException e) {
            // silently do nothing since it means that the node is already there
        }
    }

    private void addCacheChangeListener(final LoadingCache<String, EventType> eventTypeCache, LoadingCache<String, EventTypeValidator> validatorCache, final PathChildrenCache cacheSync) {
        final PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_UPDATED: {
                        invalidateCacheKey(event);
                        break;
                    }
                    case CHILD_REMOVED: {
                        invalidateCacheKey(event);
                        break;
                    }
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

    private LoadingCache<String, EventTypeValidator> setupInMemoryValidatorCache(LoadingCache<String, EventType> eventTypeCache) {
        final CacheLoader<String, EventTypeValidator> loader = new CacheLoader<String, EventTypeValidator>() {
            public EventTypeValidator load(final String key) throws Exception {
                final EventType et = eventTypeCache.get(key);
                final EventTypeValidator etv = new EventTypeValidator(et);
                final ValidationStrategyConfiguration vsc = new ValidationStrategyConfiguration();
                vsc.setStrategyName(EventBodyMustRespectSchema.NAME);
                return etv.withConfiguration(vsc);
            }
        };

        return CacheBuilder.newBuilder().maximumSize(100000).build(loader);
    }

    private LoadingCache<String,EventType> setupInMemoryEventTypeCache(final EventTypeRepository dbRepo) {
        final CacheLoader<String, EventType> loader = new CacheLoader<String, EventType>() {
            public EventType load(final String key) throws Exception {
                return dbRepo.findByName(key);
            }
        };

        return CacheBuilder.newBuilder().maximumSize(100000).build(loader);
    }

    private String getZNodePath(final String eventTypeName) {
        return ZKPaths.makePath(ZKNODE_PATH, eventTypeName);
    }
}
