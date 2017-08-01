package org.zalando.nakadi.security;

import java.util.Set;
import org.zalando.nakadi.exceptions.IllegalScopeException;

public abstract class Client {

    private final String clientId;

    public Client(final String clientId) {
        this.clientId = clientId;
    }

    public abstract void checkScopes(Set<String> allowedScopes) throws IllegalScopeException;

    public String getClientId() {
        return clientId;
    }
}
