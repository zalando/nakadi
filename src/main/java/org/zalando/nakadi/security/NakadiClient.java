package org.zalando.nakadi.security;

import java.util.Set;

public class NakadiClient extends Client {

    private final Set<String> scopes;

    public NakadiClient(final String clientId, final Set<String> scopes) {
        super(clientId);
        this.scopes = scopes;
    }
}
