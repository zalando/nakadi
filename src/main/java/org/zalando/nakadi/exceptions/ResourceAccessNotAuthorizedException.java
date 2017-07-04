package org.zalando.nakadi.exceptions;

import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

public class ResourceAccessNotAuthorizedException extends MyNakadiRuntimeException1 {
    private final AuthorizationService.Operation operation;
    private final Resource resource;

    public ResourceAccessNotAuthorizedException(
            final AuthorizationService.Operation operation, final Resource resource) {
        super(operation + " is not allowed for " + resource);
        this.operation = operation;
        this.resource = resource;
    }

    public AuthorizationService.Operation getOperation() {
        return operation;
    }

    public Resource getResource() {
        return resource;
    }
}
