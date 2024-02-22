package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationServiceFactory;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

public class TokenAuthorizationServiceFactory implements AuthorizationServiceFactory {

    @Override
    public AuthorizationService init(final SystemProperties properties) {
        final ServiceFactory serviceFactory = new ServiceFactory(properties);
        final String usersType = serviceFactory.getProperty("nakadi.plugins.authz.users-type");
        final String servicesType = serviceFactory.getProperty("nakadi.plugins.authz.services-type");
        final String businessPartnersType = serviceFactory.getProperty("nakadi.plugins.authz.business-partners-type");

        final List<String> merchantUids = Arrays.asList(
                serviceFactory.getProperty("nakadi.plugins.authz.merchant.uids").split("\\s*,\\s*"));

        try {
            return new TokenAuthorizationService(
                    usersType, serviceFactory.getOrCreateUserRegistry(),
                    servicesType, serviceFactory.getOrCreateApplicationRegistry(),
                    businessPartnersType, serviceFactory.getOrCreateMerchantRegistry(),
                    teamService(serviceFactory),
                    merchantUids);
        } catch (URISyntaxException e) {
            throw new PluginException(e);
        }
    }

    public ZalandoTeamService teamService(final ServiceFactory serviceFactory) {
        try {
            return new ZalandoTeamService(
                    serviceFactory.getProperty("nakadi.plugins.authz.teams-endpoint"),
                    serviceFactory.getOrCreateHttpClient(),
                    serviceFactory.getOrCreateTokenProvider(),
                    new ObjectMapper());
        } catch (URISyntaxException e) {
            throw new PluginException(e);
        }
    }
}
