package de.zalando.aruha.nakadi.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);
    public static final String CLOSE_CONNECTIONS_WITH_CRUTCH = "close_crutch";

    private static final String PREFIX = "/nakadi/feature_toggle/";

    private final boolean forceEnableAll;
    private final ZooKeeperHolder zkHolder;
    private final LoadingCache<Feature, Boolean> cache;

    public FeatureToggleService(final boolean forceEnableAll, final ZooKeeperHolder zkHolder) {
        this.forceEnableAll = forceEnableAll;
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
        if (forceEnableAll) {
            return feature.getDefault();
        }
        return cachedValues.getOrCalculate(feature, this::isFeatureEnabledInZk);
    }

    private Boolean isFeatureEnabledInZk(final Feature feature) {
        try {
            final Stat stat = zkHolder.get().checkExists().forPath(PREFIX + feature.getId());
            return stat != null;
        } catch (final Exception e) {
            LOG.warn("Error occurred when checking if feature '" + feature.getId() + "' is toggled", e);
            return feature.getDefault();
        }
    }

    public enum Feature {

        DISABLE_EVENT_TYPE_CREATION("disable_event_type_creation", false),
        DISABLE_EVENT_TYPE_DELETION("disable_event_type_deletion", false),
        HIGH_LEVEL_API("high_level_api", true);

        private final String id;
        private final boolean defaultValue;

        Feature(String id, boolean defaultValue) {
            this.id = id;
            this.defaultValue = defaultValue;
        }

        public String getId() {
            return id;
        }

        public boolean getDefault() {
            return defaultValue;
        }
    }
}
