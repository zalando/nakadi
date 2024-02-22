package org.zalando.nakadi.plugin.auth.utils;

import org.zalando.nakadi.plugin.api.authz.EventTypeAuthz;

public class SimpleEventResource implements EventTypeAuthz {

    private String authCompatibilityMode;

    @Override
    public String getAuthCompatibilityMode() {
        return authCompatibilityMode;
    }

    @Override
    public String getAuthCleanupPolicy() {
        return authCleanupPolicy;
    }

    private String authCleanupPolicy;

    public SimpleEventResource(final String authCompatibilityMode, final String authCleanupPolicy) {
        this.authCleanupPolicy = authCleanupPolicy;
        this.authCompatibilityMode = authCompatibilityMode;
    }

}
