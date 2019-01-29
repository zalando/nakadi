package org.zalando.nakadi.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.common.exceptions.UnauthorizedUserException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.FeatureToggleService;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;

import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;

@Component
public class ClientResolver implements HandlerMethodArgumentResolver {

    private static final String FULL_ACCESS_CLIENT_ID = "adminClientId";
    private final SecuritySettings settings;
    private final AuthorizationService authorizationService;

    @Autowired
    public ClientResolver(final SecuritySettings settings,
                          final FeatureToggleService featureToggleService,
                          final AuthorizationService authorizationService) {
        this.settings = settings;
        this.authorizationService = authorizationService;
    }

    @Override
    public boolean supportsParameter(final MethodParameter parameter) {
        return parameter.getParameterType().isAssignableFrom(Client.class);
    }

    @Override
    public Client resolveArgument(final MethodParameter parameter,
                                  final ModelAndViewContainer mavContainer,
                                  final NativeWebRequest request,
                                  final WebDataBinderFactory binderFactory) throws Exception {
        final Optional<String> clientId = Optional.ofNullable(authorizationService.getSubject().isPresent() ?
                        authorizationService.getSubject().get().getName():"unauthenticated");
        if (clientId.filter(settings.getAdminClientId()::equals).isPresent()
                || settings.getAuthMode() == OFF) {
            return new FullAccessClient(clientId.orElse(FULL_ACCESS_CLIENT_ID));
        }

        return clientId.map(client -> new NakadiClient(client, getRealm()))
                .orElseThrow(() -> new UnauthorizedUserException("Client unauthorized"));
    }

    public String getRealm() {
        try {
            return Optional.of(SecurityContextHolder.getContext())
                    .map(SecurityContext::getAuthentication)
                    .map(authentication -> (OAuth2Authentication) authentication)
                    .map(OAuth2Authentication::getUserAuthentication)
                    .map(Authentication::getDetails)
                    .map(details -> (Map) details)
                    .map(details -> details.get("realm"))
                    .map(realm -> (String) realm)
                    .orElse("");
        } catch (final ClassCastException e) {
            return "";
        }
    }
}
