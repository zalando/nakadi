package org.zalando.nakadi.service.subscription.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.zalando.nakadi.exceptions.NakadiWrapperException;

import java.util.List;
import java.util.function.Function;

public abstract class ZkSubscriptionImpl<ReturnType, ZkType> implements ZkSubscription<ReturnType>, Watcher {
    protected final CuratorFramework curatorFramework;
    protected final String key;
    private volatile Runnable listener;
    private volatile ExceptionOrData<ReturnType> data;
    private final Function<ZkType, ReturnType> converter;

    private static class ExceptionOrData<T> {
        private final NakadiWrapperException ex;
        private final T data;

        ExceptionOrData(final NakadiWrapperException ex) {
            this.ex = ex;
            this.data = null;
        }

        ExceptionOrData(final T data) {
            this.data = data;
            this.ex = null;
        }

        public T get() throws NakadiWrapperException {
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
            final String key) {
        this.listener = listener;
        this.curatorFramework = curatorFramework;
        this.key = key;
        this.converter = converter;
        this.data = null;
    }

    @Override
    public ReturnType getData() throws NakadiWrapperException {
        if (data == null) { // If there is new value pending
            try {
                // create listener only in case if subscription is still active.
                final ZkType zkData = query(null != listener);
                data = new ExceptionOrData<>(converter.apply(zkData));
            } catch (NakadiWrapperException ex) {
                data = new ExceptionOrData<>(ex);
            }
        }
        return data.get();
    }

    @Override
    public void close() {
        listener = null;
    }

    protected abstract ZkType query(boolean createListener) throws NakadiWrapperException;

    @Override
    public void process(final WatchedEvent event) {
        // on this call one actually notifies that data has changed and waits for refresh call.
        // The reason for that is that sometimes it is not possible to query data from zk while being called from
        // notification callback.
        data = null;
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
                final String key) throws NakadiWrapperException {
            super(curatorFramework, listener, converter, key);
            // The very first call is used to initialize listener
            getData();
        }

        @Override
        protected byte[] query(final boolean setListener) throws NakadiWrapperException {
            final GetDataBuilder builder = curatorFramework.getData();
            if (setListener) {
                builder.usingWatcher(this);
            }
            try {
                return builder.forPath(key);
            } catch (final Exception ex) {
                throw new NakadiWrapperException(ex);
            }
        }
    }

    public static class ZkSubscriptionChildrenImpl extends ZkSubscriptionImpl<List<String>, List<String>> {

        public ZkSubscriptionChildrenImpl(
                final CuratorFramework curatorFramework,
                final Runnable listener,
                final String key) throws NakadiWrapperException {
            super(curatorFramework, listener, Function.identity(), key);
            getData();
        }

        @Override
        protected List<String> query(final boolean setListener) throws NakadiWrapperException {
            final GetChildrenBuilder builder = curatorFramework.getChildren();
            if (setListener) {
                builder.usingWatcher(this);
            }
            try {
                return builder.forPath(key);
            } catch (final Exception ex) {
                throw new NakadiWrapperException(ex);
            }
        }
    }
}
