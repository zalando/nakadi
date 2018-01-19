package org.zalando.nakadi.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public class FeatureToggleServiceZk implements FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);
    private static final String PREFIX = "/nakadi/feature_toggle/";

    private final ZooKeeperHolder zkHolder;
    private TreeCache featuresCache;

    public FeatureToggleServiceZk(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;
    }

    @PostConstruct
    public void init() {
        try {
            this.featuresCache = TreeCache.newBuilder(zkHolder.get(), PREFIX).setCacheData(false).build();
            this.featuresCache.start();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.featuresCache.close();
    }

    public boolean isFeatureEnabled(final Feature feature) {
        try {
            return featuresCache.getCurrentData(PREFIX + feature.getId()) != null;
        } catch (final Exception e) {
            LOG.warn("Error occurred when checking if feature '" + feature.getId() + "' is toggled", e);
            return false;
        }
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
