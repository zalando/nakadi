package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationServiceFactory;

public class DefaultAuthorizationServiceFactory implements AuthorizationServiceFactory {

    @Override
    public AuthorizationService init(final SystemProperties systemProperties) {
        return new DefaultAuthorizationService();
    }
}
