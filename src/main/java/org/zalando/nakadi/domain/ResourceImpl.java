package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ResourceImpl<T> implements Resource<T> {

    public static final String ALL_DATA_ACCESS_RESOURCE = "all_data_access";
    public static final String ADMIN_RESOURCE = "nakadi";
    public static final String EVENT_TYPE_RESOURCE = "event-type";
    public static final String SUBSCRIPTION_RESOURCE = "subscription";
    public static final String PERMISSION_RESOURCE = "permission";

    private final T resource;
    private final String name;
    private final String type;
    private final ValidatableAuthorization authorization;

    public ResourceImpl(final String name, final String type,
                        final ValidatableAuthorization authorization, final T resource) {
        this.name = name;
        this.type = type;
        this.authorization = authorization;
        this.resource = resource;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        return authorization.getAttributesForOperation(operation);
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> getAuthorization() {
        return authorization.asMapValue();
    }

    @Override
    public T get() {
        return resource;
    }

    @Override
    public String toString() {
        return "AuthorizedResource{" + type + "='" + name + "'}";
    }
}
