package org.zalando.nakadi.service.subscription.state;

import org.zalando.nakadi.service.subscription.StreamingContext;

import javax.annotation.Nullable;
import java.io.IOException;

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

            try {
                getContext().getZkClient().close();
            } catch (final IOException e) {
                getLog().error("Unexpected fail to release zk connection", e);
            }
            getContext().getCurrentSpan().finish();
        }
    }
}
