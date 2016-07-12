package de.zalando.aruha.nakadi.security;

import de.zalando.aruha.nakadi.config.SecuritySettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.security.oauth2.common.exceptions.UnauthorizedUserException;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.security.Principal;
import java.util.Optional;

public class ClientIdResolver implements HandlerMethodArgumentResolver {

    private static final String CLIENT_ID = "client_id";

    private final SecuritySettings settings;

    @Autowired
    public ClientIdResolver(SecuritySettings settings) {
        this.settings = settings;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType().isAssignableFrom(String.class) && parameter.hasParameterAnnotation(Client.class);
    }

    @Override
    public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                  NativeWebRequest request, WebDataBinderFactory binderFactory) throws Exception
    {
        Optional<String> principal = Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName);
        if (settings.getAuthMode() == SecuritySettings.AuthMode.OFF) {
            return principal.orElseGet(() -> Optional.ofNullable(request.getParameter(CLIENT_ID))
                    .orElse(settings.getDefaultClientId()));
        }
        return principal.orElseThrow(() -> new UnauthorizedUserException("Client unauthorized"));
    }
}
