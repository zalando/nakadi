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

public class ClientResolver implements HandlerMethodArgumentResolver {

    private final SecuritySettings settings;

    @Autowired
    public ClientResolver(SecuritySettings settings) {
        this.settings = settings;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType().isAssignableFrom(Client.class);
    }

    @Override
    public Client resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                  NativeWebRequest request, WebDataBinderFactory binderFactory) throws Exception
    {
        Optional<Client> principal = Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName)
                .map(Client.Authorized::new);
        if (settings.getAuthMode() == SecuritySettings.AuthMode.OFF) {
            return principal.orElseGet(() -> Client.OFF_MODE);
        }
        return principal.orElseThrow(() -> new UnauthorizedUserException("Client unauthorized"));
    }
}
