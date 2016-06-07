package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.StreamingContext;

public abstract class State {
    protected final StreamingContext context;

    protected State(final StreamingContext context) {
        this.context = context;
    }

    public abstract void onEnter();

    public void onExit() {
    }

    final boolean isCurrent() {
        return context.isInState(this);
    }

}
