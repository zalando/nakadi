package org.zalando.nakadi.service;

import org.apache.commons.collections.map.HashedMap;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.KeeperException;
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
    private static final long RETRY_AFTER_SEC = 300;

    private final EventTypeCache eventTypeCache;
    private final ZooKeeperHolder zooKeeperHolder;
    private TreeCache floodersCache;

    @Autowired
    public FloodService(final EventTypeCache eventTypeCache,
                        final ZooKeeperHolder zooKeeperHolder) {
        this.eventTypeCache = eventTypeCache;
        this.zooKeeperHolder = zooKeeperHolder;
    }

    @PostConstruct
    public void initIt() {
        try {
            this.floodersCache = new TreeCache(zooKeeperHolder.get(), PATH_FLOODER);
            this.floodersCache.start();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.floodersCache.close();
    }

    public boolean isProductionBlocked(final String name) {
        return isBlocked(PATH_FLOODER_PRODUCER_ET, name) ||
                isAppBlocked(PATH_FLOODER_PRODUCER_APP, name);
    }

    public boolean isConsumptionBlocked(final String name) {
        return isBlocked(PATH_FLOODER_CONSUMER_ET, name) ||
                isAppBlocked(PATH_FLOODER_CONSUMER_APP, name);
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
        return String.valueOf(RETRY_AFTER_SEC);
    }

    public void blockFlooder(final Flooder flooder) throws RuntimeException {
        try {
            zooKeeperHolder.get().create().creatingParentsIfNeeded().forPath(createFlooderPath(flooder));
        } catch (final KeeperException.NoNodeException nne) {
            LOG.debug("Flooder does not exist {}", flooder.getName());
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while creating node in zk", e);
        }
    }

    public void unblockFlooder(final Flooder flooder) throws RuntimeException {
        try {
            zooKeeperHolder.get().delete().forPath(createFlooderPath(flooder));
        } catch (Exception e) {
            throw new RuntimeException("Issue occurred while deleting node from zk", e);
        }
    }

    private Set<String> getChildren(final String path) {
        Map<String, ChildData> currentChildren = floodersCache.getCurrentChildren(path);
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
        return floodersCache.getCurrentData(path + "/" + name) == null ? false : true;
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

        public Flooder(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public FloodService.Type getType() {
            return type;
        }

        public void setType(FloodService.Type type) {
            this.type = type;
        }
    }

}
