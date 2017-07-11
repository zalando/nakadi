package org.zalando.nakadi.service.subscription.state;

import javax.annotation.Nullable;
import org.zalando.nakadi.service.subscription.StreamingContext;

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
            getContext().unregisterAuthorizationUpdates();
        } catch (final RuntimeException ex) {
            getLog().error("Unexpected fail during removing callback for registration updates", ex);
        }
        try {
            if (null != exception) {
                getOut().onException(exception);
            }
        } finally {
            try {
                getContext().unregisterSession();
            } finally {
                switchState(StreamingContext.DEAD_STATE);
            }
        }
    }
}
