package org.zalando.nakadi.plugin;

public class DefaultTerminationService implements TerminationService {

    public void register(final String listenerName, final TerminationListener terminationRunnable) {
        // skip implementation for the local setup
    }

    public void deregister(final String listenerName) {
        // skip implementation for the local setup
    }
}
