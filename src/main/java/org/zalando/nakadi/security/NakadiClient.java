package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public class NakadiClient implements Client {

    private final String clientId;
    private final Set<String> scopes;

    public NakadiClient(final String clientId, final Set<String> scopes) {
        this.clientId = clientId;
        this.scopes = scopes;
    }

    @Override
    public boolean idMatches(final String clientId) {
        return this.clientId.equals(clientId);
    }

    @Override
    public void checkScopes(final Set<String> allowedScopes) throws IllegalScopeException {
        if (!allowedScopes.isEmpty()) {
            allowedScopes.stream()
                    .filter(scopes::contains)
                    .findAny()
                    .orElseThrow(() -> new IllegalScopeException(allowedScopes));
        }
    }

}
