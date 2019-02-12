package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

public class AccessDeniedException extends NakadiBaseException {
    private final Resource resource;
    private final AuthorizationService.Operation operation;

    public AccessDeniedException(final AuthorizationService.Operation operation, final Resource resource) {
        this.resource = resource;
        this.operation = operation;
    }

    public AccessDeniedException(final Resource resource) {
        this.resource = resource;
        this.operation = null;
    }

    public Resource getResource() {
        return resource;
    }

    public AuthorizationService.Operation getOperation() {
        return operation;
    }

    public String explain() {
        return "Access on " + operation + " " + resource.getType() + ":" + resource.getName() + " denied";
    }
}
