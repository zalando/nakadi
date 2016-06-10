package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.KafkaClient;
import de.zalando.aruha.nakadi.service.subscription.StreamParameters;
import de.zalando.aruha.nakadi.service.subscription.StreamingContext;
import de.zalando.aruha.nakadi.service.subscription.SubscriptionOutput;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClient;
import java.util.concurrent.TimeUnit;

public abstract class State {
    private StreamingContext context;

    public final void setContext(final StreamingContext context) {
        this.context = context;
    }

    public abstract void onEnter();

    public void onExit() {
    }

    final boolean isCurrent() {
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

    protected KafkaClient getKafka() {
        return context.getKafkaClient();
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

    public void scheduleTask(final Runnable task, final long timeout, final TimeUnit unit) {
        context.scheduleTask(task, timeout, unit);
    }

    public void addTask(final Runnable task) {
        context.addTask(task);
    }

    protected boolean registerSession() {
        return context.registerSession();
    }

    protected void unregisterSession() {
        context.unregisterSession();
    }
}
