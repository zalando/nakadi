package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.Subject;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class DefaultAuthorizationService implements AuthorizationService {

    @Override
    public boolean isAuthorized(final Operation operation, final Resource resource) {
        return true;
    }

    @Override
    public boolean isAuthorizationAttributeValid(final Resource resource) {
        return true;
    }

    @Override
    @Nullable
    public Optional<Subject> getSubject() {
        return Optional.empty();
    }

    @Override
    public List<Resource> filter(final List<Resource> input) throws PluginException {
        return input;
    }
}
