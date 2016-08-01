package org.zalando.nakadi.security;

import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ScopeHelper {

    public static void checkScopes(final Set<String> allowedScopes, final Set<String> clientScopes) {
        if (isNotEmptyScope(allowedScopes)){
            if (hasNoAllowedScopes(allowedScopes, clientScopes)) {
                throw new IllegalScopeException(getMissingScopes(allowedScopes, clientScopes));
            }
        }
    }

    private static boolean isNotEmptyScope(final Set<String> allowedScopes) {
        return Objects.nonNull(allowedScopes) && !allowedScopes.isEmpty();
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
