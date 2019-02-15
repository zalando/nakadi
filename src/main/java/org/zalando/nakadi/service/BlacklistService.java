package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Component
public class BlacklistService {

    private static final Logger LOG = LoggerFactory.getLogger(BlacklistService.class);
    private static final String PATH_BLACKLIST = "/nakadi/blacklist";

    private final SubscriptionDbRepository subscriptionDbRepository;
    private final ZooKeeperHolder zooKeeperHolder;
    private final NakadiAuditLogPublisher auditLogPublisher;
    private TreeCache blacklistCache;

    @Autowired
    public BlacklistService(final SubscriptionDbRepository subscriptionDbRepository,
                            final ZooKeeperHolder zooKeeperHolder,
                            final NakadiAuditLogPublisher auditLogPublisher) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.subscriptionDbRepository = subscriptionDbRepository;
        this.auditLogPublisher = auditLogPublisher;
    }

    @PostConstruct
    public void initIt() {
        try {
            this.blacklistCache =
                    TreeCache.newBuilder(zooKeeperHolder.get(), PATH_BLACKLIST).setCacheData(false).build();
            this.blacklistCache.start();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.blacklistCache.close();
    }

    private boolean isBlocked(final Type type, final String name) {
        try {
            final boolean blocked = blacklistCache.getCurrentData(type.getZkPath() + "/" + name) != null;
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
        } catch (final NoSuchSubscriptionException e) {
            // It's fine, subscription doesn't exists.
        } catch (final ServiceTemporarilyUnavailableException e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    public boolean isSubscriptionConsumptionBlocked(final Collection<String> etNames, final String appId) {
        return etNames.stream()
                .map(etName -> isBlocked(Type.CONSUMER_ET, etName)).findFirst().orElse(false) ||
                isBlocked(Type.CONSUMER_APP, appId);
    }

    public Map<String, Map<String, Set<String>>> getBlacklist() {
        return ImmutableMap.of(
                "consumers", ImmutableMap.of(
                        "event_types", getChildren(Type.CONSUMER_ET),
                        "apps", getChildren(Type.CONSUMER_APP)),
                "producers", ImmutableMap.of(
                        "event_types", getChildren(Type.PRODUCER_ET),
                        "apps", getChildren(Type.PRODUCER_APP)));
    }

    public void blacklist(final String name, final Type type) throws RuntimeException {
        try {
            final boolean oldValue = isBlocked(type, name);

            final CuratorFramework curator = zooKeeperHolder.get();
            final String path = createBlacklistEntryPath(name, type);
            if (curator.checkExists().forPath(path) == null) {
                curator.create().creatingParentsIfNeeded().forPath(path);
            }

            final BlacklistEntry newEntry = new BlacklistEntry(type, name);
            BlacklistEntry oldEntry = null;
            NakadiAuditLogPublisher.ActionType actionType = NakadiAuditLogPublisher.ActionType.CREATED;
            if (oldValue) {
                oldEntry = newEntry;
                actionType = NakadiAuditLogPublisher.ActionType.UPDATED;
            }
            auditLogPublisher.publish(
                    Optional.ofNullable(oldEntry),
                    Optional.of(newEntry),
                    NakadiAuditLogPublisher.ResourceType.BLACKLIST_ENTRY,
                    actionType,
                    newEntry.getId());
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while creating node in zk", e);
        }
    }

    public void whitelist(final String name, final Type type) throws RuntimeException {
        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            final String path = createBlacklistEntryPath(name, type);
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().forPath(path);

                final BlacklistEntry entry = new BlacklistEntry(type, name);
                auditLogPublisher.publish(
                        Optional.of(entry),
                        Optional.empty(),
                        NakadiAuditLogPublisher.ResourceType.BLACKLIST_ENTRY,
                        NakadiAuditLogPublisher.ActionType.DELETED,
                        entry.getId());
            }
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while deleting node from zk", e);
        }
    }

    private Set<String> getChildren(final Type type) {
        final Map<String, ChildData> currentChildren = blacklistCache.getCurrentChildren(type.getZkPath());
        return currentChildren == null ? Collections.emptySet() : currentChildren.keySet();
    }

    private String createBlacklistEntryPath(final String name, final Type type) {
        return type.getZkPath() + "/" + name;
    }

    public enum Type {
        CONSUMER_APP("/nakadi/blacklist/consumers/apps"),
        CONSUMER_ET("/nakadi/blacklist/consumers/event_types"),
        PRODUCER_APP("/nakadi/blacklist/producers/apps"),
        PRODUCER_ET("/nakadi/blacklist/producers/event_types");

        private final String zkPath;

        Type(final String zkPath) {
            this.zkPath = zkPath;
        }

        public String getZkPath() {
            return zkPath;
        }
    }

    public static class BlacklistEntry {
        private Type type;
        private String name;

        public BlacklistEntry(final Type type, final String name) {
            this.type = type;
            this.name = name;
        }

        public Type getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public String getId() {
            return String.format("%s:%s", type, name);
        }
    }

}
