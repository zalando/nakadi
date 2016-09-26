package org.zalando.nakadi.service;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class FloodBlocker {

    private static final Logger LOG = LoggerFactory.getLogger(FloodBlocker.class);
    private static final String PATH_FLOODER = "/nakadi/flooder";
    private static final String PATH_FLOODER_CONSUMER_APP = PATH_FLOODER + "/consumer/app/";
    private static final String PATH_FLOODER_PRODUCER_APP = PATH_FLOODER + "/producer/app/";
    private static final String PATH_FLOODER_CONSUMER_ET = PATH_FLOODER + "/consumer/event_type/";
    private static final String PATH_FLOODER_PRODUCER_ET = PATH_FLOODER + "/producer/event_type/";
    private static final long RETRY_AFTER_SEC = 300;

    private final EventTypeCache eventTypeCache;
    private final ZooKeeperHolder zooKeeperHolder;
    private TreeCache floodersCache;

    @Autowired
    public FloodBlocker(final EventTypeCache eventTypeCache,
                        final ZooKeeperHolder zooKeeperHolder) {
        this.eventTypeCache = eventTypeCache;
        this.zooKeeperHolder = zooKeeperHolder;
    }

    @PostConstruct
    public void initIt() {
        this.floodersCache = new TreeCache(zooKeeperHolder.get(), PATH_FLOODER);
        try {
            this.floodersCache.start();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.floodersCache.close();
    }

    public boolean isProductionBlocked(final String etName) {
        return isBlocked(PATH_FLOODER_PRODUCER_ET + etName) ||
                isAppBlocked(etName, PATH_FLOODER_PRODUCER_APP);
    }

    public boolean isConsumptionBlocked(final String etName) {
        return isBlocked(PATH_FLOODER_CONSUMER_ET + etName) ||
                isAppBlocked(etName, PATH_FLOODER_CONSUMER_APP);
    }

    private boolean isAppBlocked(String etName, String path) {
        try {
            final EventType eventType = eventTypeCache.getEventType(etName);
            return isBlocked(path + eventType.getOwningApplication());
        } catch (final NakadiException ne) {
            LOG.error(ne.getMessage(), ne);
        }
        return false;
    }

    private boolean isBlocked(final String path) {
        final ChildData childData = floodersCache.getCurrentData(path);
        return childData == null ? false : true;
    }

    public String getRetryAfterStr() {
        return String.valueOf(RETRY_AFTER_SEC);
    }

}
