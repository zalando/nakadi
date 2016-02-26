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

    public EventTypeCache(EventTypeRepository dbRepo, CuratorFramework zkClient) throws Exception {
        initParentCacheZNode(zkClient);

        this.zkClient = zkClient;
        this.cache = setupInMemoryCache(dbRepo);
        this.cacheSync = setupCacheSync(zkClient);
    }

    public void updated(String name) {
        try {
            String path = getZNodePath(name);
            zkClient.setData().forPath(path, new byte[0]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public EventType get(String name) throws NoSuchEventTypeException {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchEventTypeException) {
                NoSuchEventTypeException noSuchEventTypeException = (NoSuchEventTypeException) e.getCause();
                throw noSuchEventTypeException;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void created(EventType eventType) {
        try {
            String path = getZNodePath(eventType.getName());
            zkClient
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, new byte[0]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void removed(String name) {
        try {
            String path = ZKPaths.makePath(ZKNODE_PATH, name);
            zkClient.delete().forPath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initParentCacheZNode(CuratorFramework zkClient) throws Exception {
        try {
            zkClient
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ZKNODE_PATH);
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("NODE ALREADY EXISTS");
        }
    }

    private void addCacheChangeListener(final LoadingCache<String, EventType> cache, PathChildrenCache cacheSync) {
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch ( event.getType() ) {
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

            private void invalidateCacheKey(PathChildrenCacheEvent event) {
                System.out.println("INVALIDATE "+event.getData().getPath());
                String path[] = event.getData().getPath().split("/");

                cache.invalidate(path[path.length - 1]);
            }
        };

        cacheSync.getListenable().addListener(listener);
    }

    private PathChildrenCache setupCacheSync(CuratorFramework zkClient) throws Exception {
        PathChildrenCache cacheSync = new PathChildrenCache(zkClient, ZKNODE_PATH, false);

        cacheSync.start();

        addCacheChangeListener(cache, cacheSync);

        return cacheSync;
    }

    private LoadingCache<String,EventType> setupInMemoryCache(EventTypeRepository dbRepo) {
        CacheLoader<String, EventType> loader = new CacheLoader<String, EventType>() {
            public EventType load(String key) throws NoSuchEventTypeException {
                System.out.println("CACHE MISS");
                return dbRepo.findByName(key);
            }
        };

        return CacheBuilder.newBuilder().maximumSize(100000).build(loader);
    }

    private String getZNodePath(String eventTypeName) {
        return ZKPaths.makePath(ZKNODE_PATH, eventTypeName);
    }
}
