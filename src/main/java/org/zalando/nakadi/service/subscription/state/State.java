package org.zalando.nakadi.service.subscription.state;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;

public abstract class State {
    private StreamingContext context;
    private Logger log;

    public void setContext(final StreamingContext context, final String loggingPath) {
        this.context = context;
        this.log = LoggerFactory.getLogger(loggingPath + "." + this.getClass().getSimpleName());
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

    protected boolean isSubscriptionConsumptionBlocked() {
        return context.isSubscriptionConsumptionBlocked();
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

    protected void registerSession() {
        context.registerSession();
    }

    protected void unregisterSession() {
        context.unregisterSession();
    }

    public StreamingContext getContext() {
        return context;
    }
}
