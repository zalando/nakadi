package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Objects;
import java.util.Set;

public class ScopeHelper {

    public static void checkScopes(final Set<String> allowedScopes, final Set<String> clientScopes) {
        if (areScopesEmpty(allowedScopes)) {
            return;
        }

        allowedScopes.stream()
                .filter(clientScopes::contains)
                .findAny()
                .orElseThrow(() -> new IllegalScopeException(allowedScopes));
    }

    private static boolean areScopesEmpty(final Set<String> allowedScopes) {
        return Objects.isNull(allowedScopes) || allowedScopes.isEmpty();
    }

}
