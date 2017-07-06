package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.Subject;

public class AccessDeniedException extends MyNakadiRuntimeException1 {
    private final Resource resource;
    private final AuthorizationService.Operation operation;

    public AccessDeniedException(final Subject subject, final AuthorizationService.Operation operation,
                                 final Resource resource) {
        this.resource = resource;
        this.operation = operation;
    }

    public Resource getResource() {
        return resource;
    }

    public AuthorizationService.Operation getOperation() {
        return operation;
    }
}
