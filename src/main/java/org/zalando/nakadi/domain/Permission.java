package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Permission {
    private final String resource;
    private final AuthorizationService.Operation operation;
    private final AuthorizationAttribute authorizationAttribute;

    public Permission(final String resource, final AuthorizationService.Operation operation,
                      final AuthorizationAttribute authorizationAttribute) {
        this.resource = resource;
        this.operation = operation;
        this.authorizationAttribute = authorizationAttribute;
    }

    public String getResource() {
        return resource;
    }

    public AuthorizationService.Operation getOperation() {
        return operation;
    }

    public AuthorizationAttribute getAuthorizationAttribute() {
        return authorizationAttribute;
    }
}
