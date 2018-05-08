package org.zalando.nakadi.service.subscription.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class ZkSubscriptionImpl<ReturnType, ZkType> implements ZkSubscription<ReturnType> {
    protected final CuratorFramework curatorFramework;
    protected final String key;
    private final Consumer<ZkSubscriptionImpl<?, ?>> remover;
    private volatile Runnable listener;
    private volatile ExceptionOrData<ReturnType> data;
    private volatile long lastUpdate = 0L;
    private final Function<ZkType, ReturnType> converter;
    private final AtomicInteger activeWatcherIdx = new AtomicInteger(0);

    private static class ExceptionOrData<T> {
        private final NakadiRuntimeException ex;
        private final T data;

        ExceptionOrData(final NakadiRuntimeException ex) {
            this.ex = ex;
            this.data = null;
        }

        ExceptionOrData(final T data) {
            this.data = data;
            this.ex = null;
        }

        public T get() throws NakadiRuntimeException {
            if (null != ex) {
                throw ex;
            }
            return data;
        }
    }

    public ZkSubscriptionImpl(
            final CuratorFramework curatorFramework,
            final Runnable listener,
            final Function<ZkType, ReturnType> converter,
            final String key,
            final Consumer<ZkSubscriptionImpl<?, ?>> remover) {
        this.listener = listener;
        this.curatorFramework = curatorFramework;
        this.key = key;
        this.converter = converter;
        this.remover = remover;
        this.data = null;
    }

    @Override
    public ReturnType getData() throws NakadiRuntimeException {
        if (data == null) { // If there is new value pending
            try {
                // create listener only in case if subscription is still active.
                final ZkType zkData = query(null != listener);
                data = new ExceptionOrData<>(converter.apply(zkData));
            } catch (NakadiRuntimeException ex) {
                data = new ExceptionOrData<>(ex);
            }
        }
        return data.get();
    }

    @Override
    public void close() {
        listener = null;
        this.remover.accept(this);
    }

    public void refresh(final long maxDelay) {
        final Runnable listener = this.listener;
        if (null == listener) {
            return;
        }
        final long now = System.currentTimeMillis();
        if (lastUpdate == 0 || now - lastUpdate > maxDelay) {
            data = null;
            lastUpdate = now;
            listener.run();
        }
    }

    protected abstract ZkType query(boolean createListener) throws NakadiRuntimeException;

    protected Watcher createWatcher() {
        return new WatcherImpl<>(this, activeWatcherIdx.incrementAndGet());
    }

    private static class WatcherImpl<T, V> implements Watcher {
        private final ZkSubscriptionImpl<T, V> subscr;
        private final int idx;

        private WatcherImpl(final ZkSubscriptionImpl<T, V> subscr, final int idx) {
            this.subscr = subscr;
            this.idx = idx;
        }

        @Override
        public void process(final WatchedEvent event) {
            subscr.process(this.idx, event);
        }
    }

    public void process(final int orderIdx, final WatchedEvent event) {
        // on this call one actually notifies that data has changed and waits for refresh call.
        // The reason for that is that sometimes it is not possible to query data from zk while being called from
        // notification callback.
        if (orderIdx < this.activeWatcherIdx.get()) {
            // Ignore notifications from old watchers.
            return;
        }
        data = null;
        lastUpdate = System.currentTimeMillis();
        final Runnable toNotify = listener;
        // In case if subscription is still active - notify
        if (null != toNotify) {
            toNotify.run();
        }
    }

    public static class ZkSubscriptionValueImpl<R> extends ZkSubscriptionImpl<R, byte[]> {

        public ZkSubscriptionValueImpl(
                final CuratorFramework curatorFramework,
                final Runnable listener,
                final Function<byte[], R> converter,
                final String key,
                final Consumer<ZkSubscriptionImpl<?, ?>> remover) throws NakadiRuntimeException {
            super(curatorFramework, listener, converter, key, remover);
            // The very first call is used to initialize listener
            getData();
        }

        @Override
        protected byte[] query(final boolean setListener) throws NakadiRuntimeException {
            final GetDataBuilder builder = curatorFramework.getData();
            if (setListener) {
                builder.usingWatcher(createWatcher());
            }
            try {
                return builder.forPath(key);
            } catch (final Exception ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
    }

    public static class ZkSubscriptionChildrenImpl extends ZkSubscriptionImpl<List<String>, List<String>> {

        public ZkSubscriptionChildrenImpl(
                final CuratorFramework curatorFramework,
                final Runnable listener,
                final String key,
                final Consumer<ZkSubscriptionImpl<?, ?>> remover) throws NakadiRuntimeException {
            super(curatorFramework, listener, Function.identity(), key, remover);
            getData();
        }

        @Override
        protected List<String> query(final boolean setListener) throws NakadiRuntimeException {
            final GetChildrenBuilder builder = curatorFramework.getChildren();
            if (setListener) {
                builder.usingWatcher(createWatcher());
            }
            try {
                return builder.forPath(key);
            } catch (final Exception ex) {
                throw new NakadiRuntimeException(ex);
            }
        }
    }
}
