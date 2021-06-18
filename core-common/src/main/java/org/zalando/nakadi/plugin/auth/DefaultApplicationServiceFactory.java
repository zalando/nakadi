package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.ApplicationServiceFactory;
import org.zalando.nakadi.plugin.api.SystemProperties;

public class DefaultApplicationServiceFactory implements ApplicationServiceFactory {

    @Override
    public ApplicationService init(final SystemProperties properties) {
        return new DefaultApplicationService();
    }
}
