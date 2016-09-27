package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Set;

public interface Permissions {

    Permissions FULL_ACCESS = new Permissions() {

        @Override
        public boolean isOwner(final String clientId) {
            return true;
        }

        @Override
        public void checkScopes(final Set<String> allowedScopes) {
        }
    };


    boolean isOwner(String clientId);

    void checkScopes(Set<String> allowedScopes) throws IllegalScopeException;

}
