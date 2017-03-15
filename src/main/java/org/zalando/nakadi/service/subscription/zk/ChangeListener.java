package org.zalando.nakadi.service.subscription.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ChangeListener implements ZKSubscription, Watcher {
    protected final CuratorFramework curatorFramework;
    protected final String key;
    private final Runnable listener;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean registered = new AtomicBoolean();

    protected ChangeListener(final CuratorFramework curatorFramework, final String key, final Runnable listener) {
        this.curatorFramework = curatorFramework;
        this.key = key;
        this.listener = listener;
        refresh();
    }

    @Override
    public void process(final WatchedEvent event) {
        registered.set(false);
        if (!cancelled.get()) {
            listener.run();
        }
    }

    protected abstract void setInternal() throws Exception;

    @Override
    public void refresh() {
        if (!cancelled.get() && !registered.get()) {
            try {
                registered.set(true);
                setInternal();
            } catch (final Exception e) {
                throw new NakadiRuntimeException(e);
            }
        }
    }

    @Override
    public void cancel() {
        this.cancelled.set(true);
    }

    public static ChangeListener forChildren(
            final CuratorFramework curatorFramework, final String key, final Runnable listener) {
        return new ChildrenListener(curatorFramework, key, listener);
    }

    public static ChangeListener forData(
            final CuratorFramework curatorFramework, final String key, final Runnable listener) {
        return new ValueListener(curatorFramework, key, listener);
    }

    private static class ChildrenListener extends ChangeListener {

        ChildrenListener(final CuratorFramework curatorFramework, final String key, final Runnable listener) {
            super(curatorFramework, key, listener);
        }

        @Override
        protected void setInternal() throws Exception {
            curatorFramework.getChildren().usingWatcher(this).forPath(key);
        }
    }

    private static class ValueListener extends ChangeListener {

        ValueListener(final CuratorFramework curatorFramework, final String key, final Runnable listener) {
            super(curatorFramework, key, listener);
        }

        @Override
        protected void setInternal() throws Exception {
            curatorFramework.getData().usingWatcher(this).forPath(key);
        }
    }

}
