package org.zalando.nakadi.plugin.auth.utils;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class SimpleResource<T> implements Resource<T> {

    private final String name;
    private final String type;
    private final Map<AuthorizationService.Operation, List<AuthorizationAttribute>> authorization;
    private final T resource;

    public SimpleResource(final String name, final String type,
                          @Nullable final Map<AuthorizationService.Operation, List<AuthorizationAttribute>> 
                                  authorization,
                          final T resource) {
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
        return Optional.ofNullable(authorization).map(attrs -> attrs.get(operation));
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> getAuthorization() {
        if (authorization == null) {
            return null;
        }
        return authorization.entrySet().stream()
                .collect(Collectors.toMap(k -> k.getKey().toString(), Map.Entry::getValue));
    }

    @Override
    public T get() {
        return resource;
    }
}
