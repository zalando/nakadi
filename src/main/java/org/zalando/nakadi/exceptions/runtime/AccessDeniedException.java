package org.zalando.nakadi.exceptions.runtime;

import org.apache.commons.lang3.StringUtils;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

public class AccessDeniedException extends NakadiBaseException {
    private final AuthorizationService.Operation operation;
    private final Resource resource;
    private final String reason;


    public AccessDeniedException(final AuthorizationService.Operation operation,
                                 final Resource resource, final String reason) {
        this.resource = resource;
        this.operation = operation;
        this.reason = reason;
    }

    public AccessDeniedException(final AuthorizationService.Operation operation, final Resource resource) {
        this(operation, resource, null);
    }

    public AccessDeniedException(final Resource resource) {
        this(null, resource, null);
    }


    public Resource getResource() {
        return resource;
    }

    public AuthorizationService.Operation getOperation() {
        return operation;
    }

    public String explain() {
        return "Access on " + operation + " " + resource.getType() + ":" + resource.getName() + " denied"
                + (StringUtils.isEmpty(reason) ? "" : "; Reason : " + reason);
    }
}
