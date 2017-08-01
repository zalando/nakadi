package org.zalando.nakadi.security;

import java.util.Set;
import org.zalando.nakadi.exceptions.IllegalScopeException;

public class NakadiClient extends Client {

    private final Set<String> scopes;

    public NakadiClient(final String clientId, final Set<String> scopes) {
        super(clientId);
        this.scopes = scopes;
    }
}
