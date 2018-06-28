package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ValidatableAuthorization {
    Map<String, List<AuthorizationAttribute>> asMapValue();

    Optional<List<AuthorizationAttribute>> getAttributesForOperation(AuthorizationService.Operation operation)
            throws IllegalArgumentException;

}
