package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public abstract class Client {

    private final String clientId;

    public Client(final String clientId) {
        this.clientId = clientId;
    }

    public abstract boolean idMatches(String clientId);

    public abstract void checkScopes(Set<String> allowedScopes) throws IllegalScopeException;

    public String getClientId() {
        return clientId;
    }
}
