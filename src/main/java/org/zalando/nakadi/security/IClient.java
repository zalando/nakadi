package org.zalando.nakadi.security;

import java.util.Set;

public interface IClient {

    IClient FULL_ACCESS = new IClient() {
        @Override
        public boolean authenticate(String clientId) {return true;}
        @Override
        public void authorize(Set<String> allowedScopes) {}
    };

    boolean authenticate(final String clientId);

    void authorize(final Set<String> allowedScopes);
}
