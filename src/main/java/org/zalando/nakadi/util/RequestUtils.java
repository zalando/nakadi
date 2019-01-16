package org.zalando.nakadi.util;

import org.springframework.web.context.request.NativeWebRequest;

import java.security.Principal;
import java.util.Optional;

public class RequestUtils {
    public static Optional<String> getUser(final NativeWebRequest request) {
        return Optional.ofNullable(request.getUserPrincipal()).map(Principal::getName);
    }
}
