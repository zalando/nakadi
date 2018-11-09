package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.List;
import java.util.Optional;

public class NakadiResource implements Resource {
    public static final String NAKADI_RESOURCE = "nakadi";

    private final String name;
    private final ResourceAuthorization resourceAuthorization;

    public NakadiResource(final String name, final ResourceAuthorization resourceAuthorization) {
        this.name = name;
        this.resourceAuthorization = resourceAuthorization;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return NAKADI_RESOURCE;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        return resourceAuthorization.getAttributesForOperation(operation);
    }

    @Override
    public String toString() {
        return "AuthorizedResource{" + NAKADI_RESOURCE + "='" + name + "'}";
    }
}
