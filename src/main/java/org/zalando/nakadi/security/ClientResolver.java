package org.zalando.nakadi.security;

import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.util.FeatureToggleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.security.oauth2.common.exceptions.UnauthorizedUserException;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.security.Principal;
import java.util.Optional;

@Component
public class ClientResolver implements HandlerMethodArgumentResolver {

    private final SecuritySettings settings;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public ClientResolver(final SecuritySettings settings, final FeatureToggleService featureToggleService) {
        this.settings = settings;
        this.featureToggleService = featureToggleService;
    }

    @Override
    public boolean supportsParameter(final MethodParameter parameter) {
        return parameter.getParameterType().isAssignableFrom(Client.class);
    }

    @Override
    public Client resolveArgument(final MethodParameter parameter, final ModelAndViewContainer mavContainer,
                                  final NativeWebRequest request, final WebDataBinderFactory binderFactory) throws Exception
    {
        final Optional<String> clientId = Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName);

        if (!featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.CHECK_APPLICATION_LEVEL_PERMISSIONS)
                || clientId.filter(settings.getAdminClientId()::equals).isPresent()
                || settings.getAuthMode() == SecuritySettings.AuthMode.OFF)
        {
            return Client.PERMIT_ALL;
        }
        return clientId.map(Client.Authorized::new)
                .orElseThrow(() -> new UnauthorizedUserException("Client unauthorized"));

    }

}
