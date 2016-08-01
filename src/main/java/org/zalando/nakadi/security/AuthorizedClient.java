package org.zalando.nakadi.security;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.Set;

@Immutable
public final class AuthorizedClient implements Client {

    private final String clientId;
    private final Set<String> scopes;

    public AuthorizedClient(final String clientId, final Set<String> scopes) {
        this.clientId = clientId;
        this.scopes = scopes;
    }

    @Override
    public boolean is(final String clientId) {
        return this.clientId.equals(clientId);
    }

    @Override
    public Set<String> getScopes() {
        return Collections.unmodifiableSet(this.scopes);
    }

}
