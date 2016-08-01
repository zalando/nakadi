package org.zalando.nakadi.security;

import java.util.Set;

public interface IClient {

    public static final IClient FULL_ACCESS = new IClient() {
        @Override
        public boolean authenticate(String clientId) {return true;}
        @Override
        public void authorize(Set<String> allowedScopes) {}
    };

    public boolean authenticate(final String clientId);

    public void authorize(final Set<String> allowedScopes);
}
