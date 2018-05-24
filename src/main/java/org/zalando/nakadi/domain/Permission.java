package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Permission that = (Permission) o;
        return Objects.equals(resource, that.resource) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(authorizationAttribute, that.authorizationAttribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, operation, authorizationAttribute);
    }
}
