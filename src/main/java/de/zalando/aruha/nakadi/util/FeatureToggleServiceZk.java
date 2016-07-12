package de.zalando.aruha.nakadi.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureToggleServiceZk implements FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);
    private static final String PREFIX = "/nakadi/feature_toggle/";

    private final ZooKeeperHolder zkHolder;
    private final LoadingCache<Feature, Boolean> cache;

    public FeatureToggleServiceZk(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;

        cache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(5, TimeUnit.SECONDS)
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
}
