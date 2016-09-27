package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public class NakadiPermissions implements Permissions {

    private final String clientId;
    private final Set<String> scopes;

    public NakadiPermissions(final String clientId, final Set<String> scopes) {
        this.clientId = clientId;
        this.scopes = scopes;
    }

    @Override
    public boolean isOwner(String clientId) {
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
