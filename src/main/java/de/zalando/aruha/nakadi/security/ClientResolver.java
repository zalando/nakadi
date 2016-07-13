package de.zalando.aruha.nakadi.security;

import de.zalando.aruha.nakadi.config.SecuritySettings;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.security.oauth2.common.exceptions.UnauthorizedUserException;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.security.Principal;
import java.util.Optional;

import static de.zalando.aruha.nakadi.config.SecuritySettings.AuthMode.OFF;
import static de.zalando.aruha.nakadi.util.FeatureToggleService.Feature.CHECK_APPLICATION_LEVEL_PERMISSIONS;

public class ClientResolver implements HandlerMethodArgumentResolver {

    private final SecuritySettings settings;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public ClientResolver(SecuritySettings settings, FeatureToggleService featureToggleService) {
        this.settings = settings;
        this.featureToggleService = featureToggleService;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType().isAssignableFrom(Client.class);
    }

    @Override
    public Client resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                  NativeWebRequest request, WebDataBinderFactory binderFactory) throws Exception
    {
        return Optional.ofNullable(request.getUserPrincipal())
                .map(Principal::getName)
                .map(this::resolveClient)
                .orElseThrow(() -> new UnauthorizedUserException("Client unauthorized"));
    }

    private Client resolveClient(String clientId) {
        return !featureToggleService.isFeatureEnabled(CHECK_APPLICATION_LEVEL_PERMISSIONS)
                || settings.getAuthMode() == OFF
                || settings.getAdminClientId().equals(clientId)
                ? Client.PERMIT_ALL
                : new Client.Authorized(clientId);
    }
}
