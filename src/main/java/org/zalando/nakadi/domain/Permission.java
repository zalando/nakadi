package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import javax.annotation.concurrent.Immutable;
import java.util.UUID;

@Immutable
public class Permission {
    private final UUID uuid;
    private final String resource;
    private final AuthorizationService.Operation operation;
    private final AuthorizationAttribute authorizationAttribute;

    public Permission(final UUID uuid, final String resource, final AuthorizationService.Operation operation,
                      final AuthorizationAttribute authorizationAttribute) {
        this.uuid = uuid;
        this.resource = resource;
        this.operation = operation;
        this.authorizationAttribute = authorizationAttribute;
    }

    public Permission(final String resource, final AuthorizationService.Operation operation,
                      final AuthorizationAttribute authorizationAttribute) {
        this(UUID.randomUUID(), resource, operation, authorizationAttribute);
    }

    public UUID getUUID() {
        return uuid;
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
