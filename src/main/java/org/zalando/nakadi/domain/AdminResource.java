package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.List;
import java.util.Optional;

public class AdminResource implements Resource {

    public static final String ADMIN_RESOURCE = "nakadi";

    private final String name;
    private final ResourceAuthorization resourceAuthorization;

    public AdminResource(final String name, final ResourceAuthorization resourceAuthorization) {
        this.name = name;
        this.resourceAuthorization = resourceAuthorization;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return ADMIN_RESOURCE;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        return resourceAuthorization.getAttributesForOperation(operation);
    }

    @Override
    public String toString() {
        return "AuthorizedResource{" + ADMIN_RESOURCE + "='" + name + "'}";
    }

}
