package org.zalando.nakadi.security;

import java.util.Collections;
import java.util.Set;

public interface Client {

    Client PERMIT_ALL = new Client() {
        @Override
        public boolean is(final String clientId) {
            return true;
        }

        @Override
        public Set<String> getScopes() {
            return Collections.emptySet();
        }
    };

    boolean is(final String clientId);

    Set<String> getScopes();

}
