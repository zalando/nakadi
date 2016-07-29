package org.zalando.nakadi.security;

import java.util.Set;

public interface Client {

    Client PERMIT_ALL = new Client() {
        @Override
        public boolean is(String clientId) {
            return true;
        }

        @Override
        public boolean hasNoScopes(Set<String> scopes) {
            return false;
        }
    };

    boolean is(String clientId);

    boolean hasNoScopes(final Set<String> scopes);

}
