package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalClientIdException;
import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public interface Client {

    Client FULL_ACCESS = new Client() {
        @Override
        public void checkId(final String clientId) {}
        @Override
        public void checkScopes(final Set<String> allowedScopes) {}
    };

    void checkId(final String clientId) throws IllegalClientIdException;

    void checkScopes(final Set<String> allowedScopes) throws IllegalScopeException;
}
