package org.zalando.nakadi.security;

import java.util.Set;

public interface Client {

    Client PERMIT_ALL = new Client() {
        @Override
        public boolean is(final String clientId) {
            return true;
        }

        @Override
        public boolean hasNoScopes(final Set<String> scopes) {
            return false;
        }
    };

    boolean is(final String clientId);

    boolean hasNoScopes(final Set<String> scopes);

}
