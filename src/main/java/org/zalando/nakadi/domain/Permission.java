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

    public Permission(UUID uuid, String resource, AuthorizationService.Operation operation, String dataType, String value) {
        this.uuid = uuid;
        this.resource = resource;
        this.operation = operation;
        this.authorizationAttribute = new AdminAuthorizationAttribute(dataType, value);
    }

    public Permission(String resource, AuthorizationService.Operation operation, String dataType, String value) {
        this(UUID.randomUUID(), resource, operation, dataType, value);
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
