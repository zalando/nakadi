package org.zalando.nakadi.security;

import java.security.Principal;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
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
import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;

@Component
public class ClientResolver implements HandlerMethodArgumentResolver {

    private static final String FULL_ACCESS_CLIENT_ID = "adminClientId";
    private final SecuritySettings settings;

    @Autowired
    public ClientResolver(final SecuritySettings settings) {
        this.settings = settings;
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
        if (clientId.filter(settings.getAdminClientId()::equals).isPresent() || settings.getAuthMode() == OFF) {
            return new FullAccessClient(clientId.orElse(FULL_ACCESS_CLIENT_ID));
        }

        return clientId.map(client -> new NakadiClient(client, getScopes()))
                .orElseThrow(() -> new UnauthorizedUserException("Client unauthorized"));
    }

    private Set<String> getScopes() {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication instanceof OAuth2Authentication) {
            return ((OAuth2Authentication) authentication).getOAuth2Request().getScope();
        }
        return Collections.emptySet();
    }
}
