package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public interface Client {

    Client FULL_ACCESS = new Client() {
        @Override
        public boolean idMatches(final String clientId) {
            return true;
        }

        @Override
        public void checkScopes(final Set<String> allowedScopes) {
        }
    };

    boolean idMatches(String clientId);

    void checkScopes(Set<String> allowedScopes) throws IllegalScopeException;
}
