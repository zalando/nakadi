package de.zalando.aruha.nakadi.repository.db;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
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

    private final LoadingCache<String, EventType> cache;
    private final PathChildrenCache cacheSync;
    private final CuratorFramework zkClient;

    public EventTypeCache(final EventTypeRepository dbRepo, final CuratorFramework zkClient) throws Exception {
        initParentCacheZNode(zkClient);

        this.zkClient = zkClient;
        this.cache = setupInMemoryCache(dbRepo);
        this.cacheSync = setupCacheSync(zkClient);
    }

    public void updated(final String name) throws Exception {
        final String path = getZNodePath(name);
        zkClient.setData().forPath(path, new byte[0]);
    }

    public EventType get(final String name) throws NoSuchEventTypeException, ExecutionException {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchEventTypeException) {
                NoSuchEventTypeException noSuchEventTypeException = (NoSuchEventTypeException) e.getCause();
                throw noSuchEventTypeException;
            } else {
                throw e;
            }
        }
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

    private void addCacheChangeListener(final LoadingCache<String, EventType> cache, final PathChildrenCache cacheSync) {
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

                cache.invalidate(path[path.length - 1]);
            }
        };

        cacheSync.getListenable().addListener(listener);
    }

    private PathChildrenCache setupCacheSync(final CuratorFramework zkClient) throws Exception {
        final PathChildrenCache cacheSync = new PathChildrenCache(zkClient, ZKNODE_PATH, false);

        cacheSync.start();

        addCacheChangeListener(cache, cacheSync);

        return cacheSync;
    }

    private LoadingCache<String,EventType> setupInMemoryCache(final EventTypeRepository dbRepo) {
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
