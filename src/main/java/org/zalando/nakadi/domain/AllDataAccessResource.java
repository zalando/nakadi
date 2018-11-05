package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.List;
import java.util.Optional;

public class AllDataAccessResource implements Resource {
    public static final String ALL_DATA_ACCESS_RESOURCE = "all_data_access";

    private final String name;
    private final ResourceAuthorization resourceAuthorization;

    public AllDataAccessResource(final String name, final ResourceAuthorization resourceAuthorization) {
        this.name = name;
        this.resourceAuthorization = resourceAuthorization;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return ALL_DATA_ACCESS_RESOURCE;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        return resourceAuthorization.getAttributesForOperation(operation);
    }

    @Override
    public String toString() {
        return "AuthorizedResource{" + ALL_DATA_ACCESS_RESOURCE + "='" + name + "'}";
    }
}
