package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.KafkaClient;
import de.zalando.aruha.nakadi.service.subscription.StreamParameters;
import de.zalando.aruha.nakadi.service.subscription.StreamingContext;
import de.zalando.aruha.nakadi.service.subscription.SubscriptionOutput;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClient;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class State {
    private StreamingContext context;
    private Logger log;

    public void setContext(final StreamingContext context) {
        this.context = context;
        this.log = LoggerFactory.getLogger("streaming." + context.getSessionId() + "." + this.getClass().getSimpleName());
    }

    public Logger getLog() {
        return log;
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

    protected void registerSession() {
        context.registerSession();
    }

    protected void unregisterSession() {
        context.unregisterSession();
    }
}
