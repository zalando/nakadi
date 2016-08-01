package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ScopeHelper {

    public static void checkScopes(final Optional<Set<String>> allowedScope, final Set<String> clientScopes) {
        allowedScope.ifPresent(allowedScopes -> {
            if (hasNoAllowedScopes(allowedScopes, clientScopes)) {
                throw new IllegalScopeException(getMissingScopes(allowedScopes, clientScopes));
            }
        });
    }

    private static boolean hasNoAllowedScopes(final Set<String> allowedScopes, final Set<String> clientScopes) {
        return clientScopes.stream()
                .noneMatch(allowedScopes::contains);
    }

    private static Set<String> getMissingScopes(final Set<String> allowedScopes, final Set<String> clientScopes) {
        return allowedScopes.stream()
                .filter(scope -> !clientScopes.contains(scope))
                .collect(Collectors.toSet());
    }

}
