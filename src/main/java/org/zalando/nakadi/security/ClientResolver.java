package org.zalando.nakadi.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.MethodParameter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.common.exceptions.UnauthorizedUserException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.util.FeatureToggleService;

import java.security.Principal;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_APPLICATION_LEVEL_PERMISSIONS;

@Component
public class ClientResolver implements HandlerMethodArgumentResolver {

    private final SecuritySettings settings;
    private final FeatureToggleService featureToggleService;
    private final String defaultId;

    @Autowired
    public ClientResolver(final SecuritySettings settings, final FeatureToggleService featureToggleService,
                          @Value("${nakadi.oauth2.adminClientId}") final String defaultId) {
        this.settings = settings;
        this.featureToggleService = featureToggleService;
        this.defaultId = defaultId;
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
        final Optional<String> clientId = Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName);
        final String id = settings.getAuthMode() == OFF
                ? defaultId
                : clientId.orElseThrow(() -> new UnauthorizedUserException("Client unauthorized"));

        if (!featureToggleService.isFeatureEnabled(CHECK_APPLICATION_LEVEL_PERMISSIONS)
                || clientId.filter(settings.getAdminClientId()::equals).isPresent()) {
            return new Client(id, Permissions.FULL_ACCESS);
        }

        return new Client(id, new NakadiPermissions(id, getScopes()));
    }

    private Set<String> getScopes() {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication instanceof OAuth2Authentication) {
            return ((OAuth2Authentication) authentication).getOAuth2Request().getScope();
        }
        return Collections.emptySet();
    }
}
