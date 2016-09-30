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
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class FloodService {

    private static final Logger LOG = LoggerFactory.getLogger(FloodService.class);
    private static final String PATH_FLOODER = "/nakadi/flooders";

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

    public boolean isBlocked(final Type type, final String name) {
        try {
            final boolean blocked = floodersCache.getCurrentData(type + "/" + name) != null;
            if (blocked) {
                LOG.info("{} {} is blocked", type.name(), name);
            }
            return blocked;
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    public boolean isProductionBlocked(final String etName, final String appId) {
        return isBlocked(Type.PRODUCER_ET, etName) || isBlocked(Type.PRODUCER_APP, appId);
    }

    public boolean isConsumptionBlocked(final String etName, final String appId) {
        return isBlocked(Type.CONSUMER_ET, etName) || isBlocked(Type.CONSUMER_APP, appId);
    }

    public boolean isSubscriptionConsumptionBlocked(final String subscriptionId, final String appId) {
        try {
            return isSubscriptionConsumptionBlocked(
                    subscriptionDbRepository.getSubscription(subscriptionId).getEventTypes(), appId);
        } catch (final NakadiException e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    public boolean isSubscriptionConsumptionBlocked(final Collection<String> etNames, final String appId) {
            return etNames.stream()
                    .map(etName -> isBlocked(Type.CONSUMER_ET, etName)).findFirst().orElse(false) ||
                    isBlocked(Type.CONSUMER_APP, appId);
    }

    public Map<String, Map> getFlooders() {
        return new HashedMap() {
            {
                put("consumers", new HashMap<String, Set<String>>() {{
                    put("event_types", getChildren(Type.CONSUMER_ET));
                    put("apps", getChildren(Type.CONSUMER_APP));
                }});
                put("producers", new HashMap<String, Set<String>>() {{
                    put("event_types", getChildren(Type.PRODUCER_ET));
                    put("apps", getChildren(Type.PRODUCER_APP));
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

    private Set<String> getChildren(final Type type) {
        final Map<String, ChildData> currentChildren = floodersCache.getCurrentChildren(type.toString());
        return currentChildren == null ? Collections.emptySet() : currentChildren.keySet();
    }

    private String createFlooderPath(final Flooder flooder) {
        return flooder.getType() + "/" + flooder.getName();
    }

    public enum Type {
        CONSUMER_APP("/nakadi/flooders/consumers/apps"),
        CONSUMER_ET("/nakadi/flooders/consumers/event_types"),
        PRODUCER_APP("/nakadi/flooders/producers/apps"),
        PRODUCER_ET("/nakadi/flooders/producers/event_types");

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
