package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.StreamingContext;

public class CleanupState extends State {
    private final Exception exception;

    public CleanupState(final StreamingContext context, final Exception e) {
        super(context);
        this.exception = e;
    }

    CleanupState(final StreamingContext context) {
        this(context, null);
    }

    @Override
    public void onEnter() {
        try {
            if (null != exception) {
                context.out.onException(exception);
            }
        } finally {
            context.unregisterSession();
            context.switchState(null);
        }
    }
}
