package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.List;
import java.util.Optional;

public class SubscriptionResource implements Resource {
    private final String subscriptionId;
    private final SubscriptionAuthorization subscriptionAuthorization;

    public SubscriptionResource(
            final String subscriptionId, final SubscriptionAuthorization subscriptionAuthorization) {
        this.subscriptionId = subscriptionId;
        this.subscriptionAuthorization = subscriptionAuthorization;
    }

    @Override
    public String getName() {
        return subscriptionId;
    }

    @Override
    public String getType() {
        return "subscription";
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        return subscriptionAuthorization.getAttributesForOperation(operation);
    }
}
