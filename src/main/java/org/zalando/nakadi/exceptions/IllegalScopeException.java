package org.zalando.nakadi.exceptions;

import java.util.Set;

public class IllegalScopeException extends RuntimeException {

    private final Set<String> missingScopes;

    public IllegalScopeException(final Set<String> missingScopes) {
        this.missingScopes = missingScopes;
    }

    @Override
    public String getMessage() {
        return "Client has to have scopes: " + missingScopes;
    }

}