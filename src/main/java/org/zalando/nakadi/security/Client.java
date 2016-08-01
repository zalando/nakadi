package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public class Client implements IClient {

    private final String clientId;
    private final Set<String> scopes;

    public Client(final String clientId, final Set<String> scopes) {
        this.clientId = clientId;
        this.scopes = scopes;
    }

    public boolean authenticate(final String clientId) {
        return this.clientId.equals(clientId);
    }

    public void authorize(final Set<String> allowedScopes) {
        if (allowedScopes.isEmpty()) {
            allowedScopes.stream()
                    .filter(scopes::contains)
                    .findAny()
                    .orElseThrow(() -> new IllegalScopeException(allowedScopes));
        }
    }

}
