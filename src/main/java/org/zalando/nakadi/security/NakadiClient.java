package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalClientIdException;
import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public class NakadiClient implements Client {

    private final String clientId;
    private final Set<String> scopes;

    public NakadiClient(final String clientId, final Set<String> scopes) {
        this.clientId = clientId;
        this.scopes = scopes;
    }

    public void checkId(final String clientId) throws IllegalClientIdException {
        if (!this.clientId.equals(clientId)) {
            throw new IllegalClientIdException("You don't have access to this event type");
        }
    }

    public void checkScopes(final Set<String> allowedScopes) throws IllegalScopeException {
        if (!allowedScopes.isEmpty()) {
            allowedScopes.stream()
                    .filter(scopes::contains)
                    .findAny()
                    .orElseThrow(() -> new IllegalScopeException(allowedScopes));
        }
    }

}
