package org.zalando.nakadi.service.subscription.state;

import org.zalando.nakadi.service.subscription.StreamingContext;

import javax.annotation.Nullable;

public class CleanupState extends State {
    private final Exception exception;

    public CleanupState(@Nullable final Exception e) {
        this.exception = e;
    }

    CleanupState() {
        this(null);
    }

    @Override
    public void onEnter() {
        try {
            if (null != exception) {
                getOut().onException(exception);
            }
        } finally {
            try {
                unregisterSession();
            } finally {
                switchState(StreamingContext.DEAD_STATE);
            }
        }
    }
}
