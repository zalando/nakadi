package org.zalando.nakadi.plugin.auth;

import org.springframework.security.core.context.SecurityContextHolder;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.Subject;

import java.util.List;

public class DefaultAuthorizationService implements AuthorizationService {

    @Override
    public boolean isAuthorized(final Operation operation, final Resource resource) {
        return true;
    }

    @Override
    public boolean isAuthorizationAttributeValid(final AuthorizationAttribute authorizationAttribute) {
        return true;
    }

    @Override
    public Subject getSubject() {
        return () -> SecurityContextHolder.getContext().getAuthentication().getName();

    @Override
    public List<Resource> filter(final List<Resource> input) throws PluginException {
        return input;
    }
}
