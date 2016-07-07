package de.zalando.aruha.nakadi.util;

import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

public class FeatureToggleService {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureToggleService.class);

    @Value("${nakadi.featureToggle.enableAll}")
    private boolean forceEnableAll;

    private final ZooKeeperHolder zkHolder;

    public FeatureToggleService(final ZooKeeperHolder zkHolder) {
        this.zkHolder = zkHolder;
    }

    public boolean isFeatureEnabled(final Feature feature) {
        if (forceEnableAll) {
            return true;
        }
        try {
            final Stat stat = zkHolder.get().checkExists().forPath("/nakadi/feature_toggle/" + feature.getId());
            return stat != null;
        } catch (Exception e) {
            LOG.warn("Error occurred when checking if feature '" + feature.getId() + "' is toggled", e);
            return false;
        }
    }

    public static class Feature {

        public static final Feature DISABLE_EVENT_TYPE_CREATION = new Feature("disable_event_type_creation");
        public static final Feature DISABLE_EVENT_TYPE_DELETION = new Feature("disable_event_type_deletion");
        public static final Feature HIGH_LEVEL_API = new Feature("high_level_api");

        private final String id;

        private Feature(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
