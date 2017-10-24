package org.zalando.nakadi.service.subscription.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public abstract class ZkSubscrImpl<ReturnType, ZkType> implements ZkSubscr<ReturnType>, Watcher {
    protected final CuratorFramework curatorFramework;
    protected final String key;
    private final AtomicReference<Runnable> listener;
    private final AtomicReference<ExceptionOrData<ReturnType>> data = new AtomicReference<>();
    private final Function<ZkType, ReturnType> converter;

    private static class ExceptionOrData<T> {
        private final Exception ex;
        private final T data;

        ExceptionOrData(final Exception ex) {
            this.ex = ex;
            this.data = null;
        }

        ExceptionOrData(final T data) {
            this.data = data;
            this.ex = null;
        }

        public T get() throws Exception {
            if (null != ex) {
                throw ex;
            }
            return data;
        }
    }

    public ZkSubscrImpl(
            final CuratorFramework curatorFramework,
            final Runnable listener,
            final Function<ZkType, ReturnType> converter,
            final String key) {
        this.listener = new AtomicReference<>(listener);
        this.curatorFramework = curatorFramework;
        this.key = key;
        this.converter = converter;
    }

    @Override
    public ReturnType getData() throws Exception {
        if (data.get() == null) { // If there is new value pending
            try {
                // create listener only in case if subscription is still active.
                final ZkType zkData = query(null != listener.get());
                data.set(new ExceptionOrData<>(converter.apply(zkData)));
            } catch (Exception ex) {
                data.set(new ExceptionOrData<>(ex));
            }
        }
        return data.get().get();
    }

    @Override
    public void close() {
        listener.set(null);
    }

    protected abstract ZkType query(boolean createListener) throws Exception;

    @Override
    public void process(final WatchedEvent event) {
        // on this call one actually notifies that data has changed and waits for refresh call.
        // The reason for that is that sometimes it is not possible to query data from zk while being called from
        // notification callback.
        data.set(null);
        final Runnable toNotify = listener.get();
        // In case if subscription is still active - notify
        if (null != toNotify) {
            toNotify.run();
        }
    }

    public static class ZkSubscrValueImpl<R> extends ZkSubscrImpl<R, byte[]> {

        public ZkSubscrValueImpl(
                final CuratorFramework curatorFramework,
                final Runnable listener,
                final Function<byte[], R> converter,
                final String key) throws Exception {
            super(curatorFramework, listener, converter, key);
            // The very first call is used to initialize listener
            query(true);
        }

        @Override
        protected byte[] query(final boolean setListener) throws Exception {
            final GetDataBuilder builder = curatorFramework.getData();
            if (setListener) {
                builder.usingWatcher(this);
            }
            return builder.forPath(key);
        }
    }

    public static class ZkSubscrChildrenImpl extends ZkSubscrImpl<List<String>, List<String>> {

        public ZkSubscrChildrenImpl(
                final CuratorFramework curatorFramework,
                final Runnable listener,
                final String key) throws Exception {
            super(curatorFramework, listener, Function.identity(), key);
            query(true);
        }

        @Override
        protected List<String> query(final boolean setListener) throws Exception {
            final GetChildrenBuilder builder = curatorFramework.getChildren();
            if (setListener) {
                builder.usingWatcher(this);
            }
            return builder.forPath(key);
        }
    }
}
