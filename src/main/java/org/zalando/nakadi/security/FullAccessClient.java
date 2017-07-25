package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public class FullAccessClient extends Client {

    public FullAccessClient(final String clientId) {
        super(clientId);
    }

    @Override
    public void checkScopes(final Set<String> allowedScopes) throws IllegalScopeException {

    }
}
