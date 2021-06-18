package org.zalando.nakadi.service.subscription.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.service.subscription.LogPathBuilder;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.autocommit.AutocommitSupport;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public abstract class State {
    private StreamingContext context;
    private Logger log;

    public void setContext(final StreamingContext context) {
        this.context = context;
        this.log = LoggerFactory.getLogger(LogPathBuilder.build(
                context.getSubscription().getId(),
                context.getSessionId(),
                "state." + this.getClass().getSimpleName()));
    }

    public Logger getLog() {
        return log;
    }

    public abstract void onEnter();

    public void onExit() {
    }

    private boolean isCurrent() {
        return context.isInState(this);
    }

    protected long getKafkaPollTimeout() {
        return context.getKafkaPollTimeout();
    }

    protected StreamParameters getParameters() {
        return context.getParameters();
    }

    protected ZkSubscriptionClient getZk() {
        return context.getZkClient();
    }

    protected String getSessionId() {
        return context.getSessionId();
    }

    protected SubscriptionOutput getOut() {
        return context.getOut();
    }

    protected void switchState(final State newState) {
        context.switchState(newState);
    }

    protected boolean isConnectionReady() {
        return context.isConnectionReady();
    }

    public void scheduleTask(final Runnable task, final long timeout, final TimeUnit unit) {
        context.scheduleTask(linkTaskToState(task), timeout, unit);
    }

    public void addTask(final Runnable task) {
        context.addTask(linkTaskToState(task));
    }

    private Runnable linkTaskToState(final Runnable task) {
        return () -> {
            if (!isCurrent()) {
                return;
            }
            task.run();
        };
    }

    public StreamingContext getContext() {
        return context;
    }

    public Comparator<NakadiCursor> getComparator() {
        return getContext().getCursorComparator();
    }

    protected AutocommitSupport getAutocommit() {
        return getContext().getAutocommitSupport();
    }
}
