package org.zalando.nakadi.security;

import java.util.Set;
import org.zalando.nakadi.exceptions.IllegalScopeException;

public class FullAccessClient extends Client {

    public FullAccessClient(final String clientId) {
        super(clientId);
    }

    @Override
    public void checkScopes(final Set<String> allowedScopes) throws IllegalScopeException {

    }
}
