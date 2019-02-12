package org.zalando.nakadi.plugin.auth;


import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.plugin.api.exceptions.OperationOnResourceNotPermittedException;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class DefaultAuthorizationService implements AuthorizationService {

    @Override
    public boolean isAuthorized(final Operation operation, final Resource resource) throws PluginException {
        return true;
    }

    @Override
    public void isAuthorizationForResourceValid(final Resource resource) throws PluginException,
            AuthorizationInvalidException, OperationOnResourceNotPermittedException {
    }

    public DefaultAuthorizationService() {
        super();
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
