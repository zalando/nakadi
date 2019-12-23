package org.zalando.nakadi.plugin;

import org.zalando.nakadi.plugin.api.exceptions.PluginException;

public class DefaultTerminationService implements TerminationService {

    public void register(final String listenerName, final TerminationListener terminationRunnable) {
        // skip implementation for the local setup
    }

    public void deregister(final String listenerName) {
        // skip implementation for the local setup
    }

    boolean isTerminating() throws PluginException {
        return false;
    }

}
