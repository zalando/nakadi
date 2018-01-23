package org.zalando.nakadi.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PreDestroy;
import java.io.IOException;

public class FeatureToggleServiceZk implements FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);
    private static final String PREFIX = "/nakadi/feature_toggle";

    private final ZooKeeperHolder zkHolder;
    private PathChildrenCache featuresCache;

    public FeatureToggleServiceZk(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;
        try {
            this.featuresCache = new PathChildrenCache(zkHolder.get(), PREFIX, false);
            this.featuresCache.start();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        try {
            this.featuresCache.close();
        } catch (IOException e) {
            LOG.error("Could not close features cache", e);
        }
    }

    public boolean isFeatureEnabled(final Feature feature) {
        try {
            return featuresCache.getCurrentData(PREFIX + "/" + feature.getId()) != null;
        } catch (final Exception e) {
            LOG.warn("Error occurred when checking if feature '" + feature.getId() + "' is toggled", e);
            return false;
        }
    }

    public void setFeature(final FeatureWrapper feature) {
        try {
            final CuratorFramework curator = zkHolder.get();
            final String path = PREFIX + "/" + feature.getFeature().getId();
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
