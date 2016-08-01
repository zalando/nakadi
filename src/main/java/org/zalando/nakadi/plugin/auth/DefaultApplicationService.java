package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.ApplicationService;

public class DefaultApplicationService implements ApplicationService {

    @Override
    public boolean exists(final String applicationId) {
        return true;
    }
}
