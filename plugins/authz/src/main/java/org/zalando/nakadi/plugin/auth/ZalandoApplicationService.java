package org.zalando.nakadi.plugin.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

public class ZalandoApplicationService implements ApplicationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZalandoApplicationService.class);

    private final ValueRegistry applicationRegistry;

    public ZalandoApplicationService(final ValueRegistry applicationRegistry) {
        this.applicationRegistry = applicationRegistry;
    }

    @Override
    public boolean exists(final String applicationId) throws PluginException {
        if (applicationId == null) {
            LOGGER.warn("null application id is not valid");
            return false;
        }

        final String actualValueToCheck;
        if (applicationId.startsWith(TokenAuthorizationService.SERVICE_PREFIX)) {
            actualValueToCheck = applicationId.substring(TokenAuthorizationService.SERVICE_PREFIX.length());
        } else {
            actualValueToCheck = applicationId;
        }

        if (applicationRegistry.isValid(actualValueToCheck)) {
            return true;
        }
        LOGGER.info("Application is not found: {}({})", applicationId, actualValueToCheck);
        return false;
    }
}
