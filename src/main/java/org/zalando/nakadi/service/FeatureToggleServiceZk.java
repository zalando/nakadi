package org.zalando.nakadi.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class FeatureToggleServiceZk implements FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);
    private static final String PREFIX = "/nakadi/feature_toggle";

    private final ZooKeeperHolder zkHolder;

    private NakadiAuditLogPublisher auditLogPublisher;
    private PathChildrenCache featuresCache;
    private final Map<Feature, String> featurePaths;

    public FeatureToggleServiceZk(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;
        try {
            this.featuresCache = new PathChildrenCache(zkHolder.get(), PREFIX, false);
            this.featuresCache.start();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
        featurePaths = new HashMap<>(Feature.values().length);
        for (final Feature feature : Feature.values()) {
            featurePaths.put(feature, PREFIX + "/" + feature.getId());
        }
    }

    @PreDestroy
    public void cleanUp() {
        try {
            this.featuresCache.close();
        } catch (final IOException e) {
            LOG.error("Could not close features cache", e);
        }
    }

    public boolean isFeatureEnabled(final Feature feature) {
        try {
            return featuresCache.getCurrentData(getPath(feature)) != null;
        } catch (final Exception e) {
            LOG.warn("Error occurred when checking if feature '" + feature.getId() + "' is toggled", e);
            return false;
        }
    }

    public void setFeature(final FeatureWrapper feature, final Optional<String> user) {
        try {
            final boolean oldState = isFeatureEnabled(feature.getFeature());
            final CuratorFramework curator = zkHolder.get();
            final String path = getPath(feature.getFeature());
            if (feature.isEnabled()) {
                curator.create().creatingParentsIfNeeded().forPath(path);
                LOG.debug("Feature {} enabled", feature.getFeature().getId());
            } else {
                curator.delete().forPath(path);
                LOG.debug("Feature {} disabled", feature.getFeature().getId());
            }

            if (auditLogPublisher != null) {
                auditLogPublisher.publish(
                        Optional.of(new FeatureToggleService.FeatureWrapper(feature.getFeature(), oldState)),
                        Optional.of(feature),
                        NakadiAuditLogPublisher.ResourceType.FEATURE,
                        NakadiAuditLogPublisher.ActionType.UPDATED,
                        feature.getFeature().getId(),
                        user);
            }
        } catch (final KeeperException.NoNodeException nne) {
            LOG.debug("Feature {} was already disabled", feature.getFeature().getId());
        } catch (final KeeperException.NodeExistsException nne) {
            LOG.debug("Feature {} was already enabled", feature.getFeature().getId());
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while accessing zk", e);
        }
    }

    private String getPath(final Feature feature) {
        return featurePaths.get(feature);
    }

    public void setAuditLogPublisher(final NakadiAuditLogPublisher auditLogPublisher) {
        this.auditLogPublisher = auditLogPublisher;
    }
}
