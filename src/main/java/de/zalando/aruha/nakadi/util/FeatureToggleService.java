package de.zalando.aruha.nakadi.util;

import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);

    private static final String PREFIX = "/nakadi/feature_toggle/";

    private final boolean forceEnableAll;
    private final ZooKeeperHolder zkHolder;

    public FeatureToggleService(boolean forceEnableAll, final ZooKeeperHolder zkHolder) {
        this.forceEnableAll = forceEnableAll;
        this.zkHolder = zkHolder;
    }

    public boolean isFeatureEnabled(final Feature feature) {
        if (forceEnableAll) {
            return feature.getDefault();
        }
        try {
            final Stat stat = zkHolder.get().checkExists().forPath(PREFIX + feature.getId());
            return stat != null;
        } catch (Exception e) {
            LOG.warn("Error occurred when checking if feature '" + feature.getId() + "' is toggled", e);
            return false;
        }
    }

    public static class Feature {

        public static final Feature DISABLE_EVENT_TYPE_CREATION = new Feature("disable_event_type_creation", false);
        public static final Feature DISABLE_EVENT_TYPE_DELETION = new Feature("disable_event_type_deletion", false);
        public static final Feature HIGH_LEVEL_API = new Feature("high_level_api", true);

        private final String id;
        private final boolean defaultValue;

        private Feature(String id, boolean defaultValue) {
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
