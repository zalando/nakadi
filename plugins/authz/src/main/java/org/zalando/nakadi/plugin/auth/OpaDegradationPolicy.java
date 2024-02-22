package org.zalando.nakadi.plugin.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.util.Collections;
import java.util.Set;

public enum OpaDegradationPolicy {
    RESTRICT {
        @Override
        public Set<String> handle(final String requestInfo, final RuntimeException ex) {
            LOG.warn("Opa service degraded, restricting all retailer ids for {}", requestInfo, ex);
            return Collections.emptySet();
        }
    },
    PERMIT {
        @Override
        public Set<String> handle(final String requestInfo, final RuntimeException ex) {
            LOG.warn("Opa service degraded, permitting all retailer ids for {}", requestInfo, ex);
            return Collections.singleton("*");
        }
    },
    THROW {
        @Override
        public Set<String> handle(final String requestInfo, final RuntimeException ex) {
            throw new PluginException("Exception while calling OPA, message=" + ex.getMessage()
                    + " for " + requestInfo, ex);
        }
    },
    ;

    private static final Logger LOG = LoggerFactory.getLogger(OpaDegradationPolicy.class);

    public abstract Set<String> handle(String requestInfo, RuntimeException ex);


}
