package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.ApplicationServiceFactory;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.net.URISyntaxException;

public class ZalandoApplicationServiceFactory implements ApplicationServiceFactory {
    @Override
    public ApplicationService init(final SystemProperties properties) throws PluginException {
        final ServiceFactory serviceFactory = new ServiceFactory(properties);
        try {
            return new ZalandoApplicationService(serviceFactory.getOrCreateApplicationRegistry());
        } catch (final URISyntaxException ex) {
            throw new PluginException("Failed to initialize application service", ex);
        }
    }
}
