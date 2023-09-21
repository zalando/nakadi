package org.zalando.nakadi.service.subscription.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.service.subscription.StreamingContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;

public class CleanupState extends State {
    private static final Logger LOG = LoggerFactory.getLogger(CleanupState.class);
    private final Exception exception;

    public CleanupState(@Nullable final Exception e) {
        this.exception = e;
    }

    CleanupState() {
        this(null);
    }

    @Override
    public void onEnter() {
//        Arrays.stream(getZk().getTopology().getPartitions())
//                .filter(p -> p.getFailedCommitsCount() > 0)
//                .map(p -> String.format("p%s %d ", p.getPartition(), p.getFailedCommitsCount()))
//                .forEach(LOG::error);

        try {
            getContext().unregisterAuthorizationUpdates();
        } catch (final RuntimeException ex) {
            LOG.error("Unexpected fail during removing callback for registration updates", ex);
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
                getContext().closeZkClient();
            } catch (final IOException e) {
                LOG.error("Unexpected fail to release zk connection", e);
            }
        }
    }
}
