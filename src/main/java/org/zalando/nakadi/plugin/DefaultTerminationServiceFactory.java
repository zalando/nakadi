package org.zalando.nakadi.plugin;

import org.zalando.nakadi.plugin.api.SystemProperties;

public class DefaultTerminationServiceFactory {

    public DefaultTerminationService init(final SystemProperties systemProperties) {
        return new DefaultTerminationService();
    }
}
