package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public interface IClient {

    IClient FULL_ACCESS = new IClient() {
        @Override
        public boolean authenticate(final String clientId) {return true;}
        @Override
        public void authorize(final Set<String> allowedScopes) {}
    };

    boolean authenticate(final String clientId);

    void authorize(final Set<String> allowedScopes) throws IllegalScopeException;
}
