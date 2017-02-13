package org.zalando.nakadi.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class FeatureToggleServiceZk implements FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);
    private static final String PREFIX = "/nakadi/feature_toggle/";

    private final ZooKeeperHolder zkHolder;
    private final LoadingCache<Feature, Boolean> cache;

    public FeatureToggleServiceZk(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;

        cache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<Feature, Boolean>() {
                    @Override
                    public Boolean load(final Feature key) throws Exception {
                        return isFeatureEnabledInZk(key);
                    }
                });

    }

    public boolean isFeatureEnabled(final Feature feature) {
        try {
            return cache.get(feature);
        } catch (final ExecutionException e) {
            LOG.warn("Error occurred when checking if feature '" + feature.getId() + "' is toggled", e);
            return false;
        }
    }

    private Boolean isFeatureEnabledInZk(final Feature feature) throws Exception {
        return null != zkHolder.get().checkExists().forPath(PREFIX + feature.getId());
    }

    public void setFeature(final FeatureWrapper feature) {
        try {
            final CuratorFramework curator = zkHolder.get();
            final String path = PREFIX + feature.getFeature().getId();
            if (feature.isEnabled()) {
                curator.create().creatingParentsIfNeeded().forPath(path);
            } else {
                curator.delete().forPath(path);
            }
        } catch (final KeeperException.NoNodeException nne) {
            LOG.debug("Feature {} was disabled", feature.getFeature().getId());
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while accessing zk", e);
        }
    }
}
