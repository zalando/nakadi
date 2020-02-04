package org.zalando.nakadi.cache;

import com.google.common.base.Charsets;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@Service
public class ChangesRegistry {
    private final ZooKeeperHolder zk;
    private static final String ET_CACHE_PATH = "/etcache_changes";

    @Autowired
    public ChangesRegistry(final ZooKeeperHolder zk) {
        this.zk = zk;
        try {
            zk.get().create().creatingParentsIfNeeded().forPath(ET_CACHE_PATH);
        } catch (KeeperException.NodeExistsException ex) {
            // Its all fine
        } catch (Exception ex) {
            throw new NakadiRuntimeException(ex);
        }
    }

    /**
     * Return list of changes in zookeeper. in case if {@code changeListener} is provided, sets it to react
     * on changelist changes
     *
     * @param changesListener Listener to set.
     * @return Current list of changes in zookeeper
     * @throws Exception In case if there is error in communicating with zookeeper.
     */
    public List<Change> getCurrentChanges(@Nullable final Runnable changesListener) throws Exception {
        final List<String> children;
        if (null == changesListener) {
            children = zk.get().getChildren()
                    .forPath(ET_CACHE_PATH);
        } else {
            children = zk.get().getChildren()
                    .usingWatcher((CuratorWatcher) (e) -> changesListener.run())
                    .forPath(ET_CACHE_PATH);
        }
        final List<Change> changes = new ArrayList<>();
        for (final String child : children) {
            final byte[] data = zk.get().getData().forPath(getPath(child));
            changes.add(new Change(child, new String(data, Charsets.UTF_8), new Date()));
        }
        return changes;
    }

    public void registerChange(final String eventType) throws Exception {
        final String key = UUID.randomUUID().toString(); // Let's assume, that this value is unique.
        zk.get().create().forPath(getPath(key), eventType.getBytes(Charsets.UTF_8));
    }

    public void deleteChanges(final List<String> changeIds) throws Exception {
        for (final String child : changeIds) {
            try {
                zk.get().delete().forPath(getPath(child));
            } catch (KeeperException.NoNodeException ex) {
                // That's fine
            }
        }
    }

    private String getPath(final String child) {
        return ET_CACHE_PATH + "/" + child;
    }
}
