package org.zalando.nakadi.security;

import java.util.Set;

public final class AuthorizedClient implements Client {

    private final String clientId;
    private final Set<String> scopes;

    public AuthorizedClient(final String clientId, final Set<String> scopes) {
        this.clientId = clientId;
        this.scopes = scopes;
    }

    @Override
    public final boolean is(final String clientId) {
        return this.clientId.equals(clientId);
    }

    @Override
    public final boolean hasNoScopes(final Set<String> scopes) {
        return this.scopes.stream().noneMatch(scope -> scopes.contains(scope));
    }

}
