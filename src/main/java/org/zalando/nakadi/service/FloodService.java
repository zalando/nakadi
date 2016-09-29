package org.zalando.nakadi.service;

import org.apache.commons.collections.map.HashedMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class FloodService {

    private static final Logger LOG = LoggerFactory.getLogger(FloodService.class);
    private static final String PATH_FLOODER = "/nakadi/flooders";
    private static final String PATH_FLOODER_CONSUMER_APP = PATH_FLOODER + Type.CONSUMER_APP;
    private static final String PATH_FLOODER_PRODUCER_APP = PATH_FLOODER + Type.PRODUCER_APP;
    private static final String PATH_FLOODER_CONSUMER_ET = PATH_FLOODER + Type.CONSUMER_ET;
    private static final String PATH_FLOODER_PRODUCER_ET = PATH_FLOODER + Type.PRODUCER_ET;

    private final EventTypeCache eventTypeCache;
    private final SubscriptionDbRepository subscriptionDbRepository;
    private final ZooKeeperHolder zooKeeperHolder;
    private final String retryAfterStr;
    private TreeCache floodersCache;

    @Autowired
    public FloodService(final EventTypeCache eventTypeCache,
                        final SubscriptionDbRepository subscriptionDbRepository,
                        final ZooKeeperHolder zooKeeperHolder,
                        final NakadiSettings nakadiSettings) {
        this.eventTypeCache = eventTypeCache;
        this.zooKeeperHolder = zooKeeperHolder;
        this.subscriptionDbRepository = subscriptionDbRepository;
        this.retryAfterStr = String.valueOf(nakadiSettings.getRetryAfter());
    }

    @PostConstruct
    public void initIt() {
        try {
            this.floodersCache = TreeCache.newBuilder(zooKeeperHolder.get(), PATH_FLOODER).setCacheData(false).build();
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
        return isBlocked(PATH_FLOODER_PRODUCER_ET, etName) ||
                isAppBlocked(PATH_FLOODER_PRODUCER_APP, etName);
    }

    public boolean isConsumptionBlocked(final String etName) {
        return isBlocked(PATH_FLOODER_CONSUMER_ET, etName) ||
                isAppBlocked(PATH_FLOODER_CONSUMER_APP, etName);
    }

    public boolean isSubscriptionConsumptionBlocked(final String subscriptionId) {
        try {
            return subscriptionDbRepository.getSubscription(subscriptionId).getEventTypes().stream()
                    .map(etName -> isConsumptionBlocked(etName)).findFirst().orElse(false);
        } catch (final NakadiException e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    public Map<String, Map> getFlooders() {
        return new HashedMap() {
            {
                put("consumers", new HashMap<String, Set<String>>() {{
                    put("event_types", getChildren(PATH_FLOODER_CONSUMER_ET));
                    put("apps", getChildren(PATH_FLOODER_CONSUMER_APP));
                }});
                put("producers", new HashMap<String, Set<String>>() {{
                    put("event_types", getChildren(PATH_FLOODER_PRODUCER_ET));
                    put("apps", getChildren(PATH_FLOODER_PRODUCER_APP));
                }});
            }
        };
    }

    public String getRetryAfterStr() {
        return retryAfterStr;
    }

    public void blockFlooder(final Flooder flooder) throws RuntimeException {
        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            final String path = createFlooderPath(flooder);
            if (curator.checkExists().forPath(path) == null) {
                curator.create().creatingParentsIfNeeded().forPath(path);
            }
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while creating node in zk", e);
        }
    }

    public void unblockFlooder(final Flooder flooder) throws RuntimeException {
        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            final String path = createFlooderPath(flooder);
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().forPath(path);
            }
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while deleting node from zk", e);
        }
    }

    private Set<String> getChildren(final String path) {
        final Map<String, ChildData> currentChildren = floodersCache.getCurrentChildren(path);
        return currentChildren == null ? Collections.emptySet() : currentChildren.keySet();
    }

    private boolean isAppBlocked(final String path, final String etName) {
        try {
            final EventType eventType = eventTypeCache.getEventType(etName);
            return isBlocked(path, eventType.getOwningApplication());
        } catch (final NakadiException ne) {
            LOG.error(ne.getMessage(), ne);
        }
        return false;
    }

    private boolean isBlocked(final String path, final String name) {
        try {
            return floodersCache.getCurrentData(path + "/" + name) == null ? false : true;
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    private String createFlooderPath(final Flooder flooder) {
        return PATH_FLOODER + flooder.getType() + "/" + flooder.getName();
    }

    public enum Type {
        CONSUMER_APP("/consumers/apps"),
        CONSUMER_ET("/consumers/event_types"),
        PRODUCER_APP("/producers/apps"),
        PRODUCER_ET("/producers/event_types");

        private final String value;

        Type(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static class Flooder {
        private String name;
        private FloodService.Type type;

        public Flooder() {
        }

        public Flooder(final String name, final Type type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public FloodService.Type getType() {
            return type;
        }

        public void setType(final FloodService.Type type) {
            this.type = type;
        }
    }

}
