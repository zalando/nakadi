package org.zalando.nakadi.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.exceptions.IllegalScopeException;

import java.util.Optional;
import java.util.Set;

public class ScopeHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ScopeHelper.class);

    public static void checkScopes(final Optional<Set<String>> scope, final Client client) {
        scope.ifPresent(scopes -> {
            LOG.debug("scopes={}", scopes);
            if (client.hasNoScopes(scopes)) {
                throw new IllegalScopeException();
            }
        });
    }
}
